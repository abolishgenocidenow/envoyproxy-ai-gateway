// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package translator

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/envoyproxy/ai-gateway/internal/apischema/awsbedrock"
	"github.com/envoyproxy/ai-gateway/internal/apischema/openai"
	"github.com/envoyproxy/ai-gateway/internal/filterapi"
	"github.com/envoyproxy/ai-gateway/internal/internalapi"
	"github.com/envoyproxy/ai-gateway/internal/json"
	"github.com/envoyproxy/ai-gateway/internal/tracing/tracingapi"
	"github.com/envoyproxy/ai-gateway/usage"
)

// https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-anthropic-claude-messages.html
const BedrockDefaultVersion = "bedrock-2023-05-31"

// NewChatCompletionOpenAIToAWSAnthropicTranslator implements [Factory] for OpenAI to AWS Anthropic translation.
// This translator converts OpenAI ChatCompletion API requests to AWS Anthropic API format.
func NewChatCompletionOpenAIToAWSAnthropicTranslator(apiVersion string, modelNameOverride internalapi.ModelNameOverride) OpenAIChatCompletionTranslator {
	return &openAIToAWSAnthropicTranslatorV1ChatCompletion{
		apiVersion:        apiVersion,
		modelNameOverride: modelNameOverride,
	}
}

// openAIToAWSAnthropicTranslatorV1ChatCompletion translates OpenAI Chat Completions API to AWS Anthropic Claude API.
// This uses the AWS Bedrock InvokeModel and InvokeModelWithResponseStream APIs:
// https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-anthropic-claude-messages.html
type openAIToAWSAnthropicTranslatorV1ChatCompletion struct {
	amazonState       amazonStreamState
	usageDecoder      usage.Decoder
	apiVersion        string
	modelNameOverride internalapi.ModelNameOverride
	streamParser      *anthropicStreamParser
	requestModel      internalapi.RequestModel
	bufferedBody      []byte
	streamLimit       int
}

// RequestBody implements [OpenAIChatCompletionTranslator.RequestBody] for AWS Anthropic.
func (o *openAIToAWSAnthropicTranslatorV1ChatCompletion) RequestBody(_ []byte, openAIReq *openai.ChatCompletionRequest, _ bool) (
	newHeaders []internalapi.Header, newBody []byte, err error,
) {
	o.requestModel = openAIReq.Model
	if o.modelNameOverride != "" {
		o.requestModel = o.modelNameOverride
	}

	// URL encode the model name for the path to handle special characters (e.g., ARNs)
	encodedModelName := url.PathEscape(o.requestModel)

	// Set the path for AWS Bedrock InvokeModel API
	// https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_InvokeModel.html#API_runtime_InvokeModel_RequestSyntax
	pathTemplate := "/model/%s/invoke"
	if openAIReq.Stream {
		pathTemplate = "/model/%s/invoke-with-response-stream"
		o.streamParser = newAnthropicStreamParser(o.requestModel)
	}

	params, err := buildAnthropicParams(openAIReq, filterapi.APISchemaAWSAnthropic, o.modelNameOverride)
	if err != nil {
		return
	}

	body, err := json.Marshal(params)
	if err != nil {
		return
	}

	// b. Set the "anthropic_version" key in the JSON body
	// Using same logic as anthropic go SDK: https://github.com/anthropics/anthropic-sdk-go/blob/e252e284244755b2b2f6eef292b09d6d1e6cd989/bedrock/bedrock.go#L167
	anthropicVersion := BedrockDefaultVersion
	if o.apiVersion != "" {
		anthropicVersion = o.apiVersion
	}
	body, err = sjson.SetBytes(body, anthropicVersionKey, anthropicVersion)
	if err != nil {
		return
	}
	newBody = body

	newHeaders = []internalapi.Header{
		{pathHeaderName, fmt.Sprintf(pathTemplate, encodedModelName)},
		{contentLengthHeaderName, strconv.Itoa(len(newBody))},
	}
	return
}

// ResponseError implements [OpenAIChatCompletionTranslator.ResponseError].
// Translate AWS Bedrock exceptions to OpenAI error type.
// The error type is stored in the "x-amzn-errortype" HTTP header for AWS error responses.
func (o *openAIToAWSAnthropicTranslatorV1ChatCompletion) ResponseError(respHeaders map[string]string, body io.Reader) (
	newHeaders []internalapi.Header, newBody []byte, err error,
) {
	statusCode := respHeaders[statusHeaderName]
	var openaiError openai.Error
	if v, ok := respHeaders[contentTypeHeaderName]; ok && strings.Contains(v, jsonContentType) {
		var bedrockError awsbedrock.BedrockException
		if err = json.NewDecoder(body).Decode(&bedrockError); err != nil {
			return nil, nil, fmt.Errorf("failed to unmarshal error body: %w", err)
		}
		openaiError = openai.Error{
			Type: "error",
			Error: openai.ErrorType{
				Type:    respHeaders[awsErrorTypeHeaderName],
				Message: bedrockError.Message,
				Code:    &statusCode,
			},
		}
	} else {
		var buf []byte
		buf, err = io.ReadAll(body)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read error body: %w", err)
		}
		openaiError = openai.Error{
			Type: "error",
			Error: openai.ErrorType{
				Type:    awsBedrockBackendError,
				Message: string(buf),
				Code:    &statusCode,
			},
		}
	}
	newBody, err = json.Marshal(openaiError)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal error body: %w", err)
	}
	newHeaders = []internalapi.Header{
		{contentTypeHeaderName, jsonContentType},
		{contentLengthHeaderName, strconv.Itoa(len(newBody))},
	}
	return
}

// ResponseHeaders implements [OpenAIChatCompletionTranslator.ResponseHeaders].
func (o *openAIToAWSAnthropicTranslatorV1ChatCompletion) ResponseHeaders(_ map[string]string) (
	newHeaders []internalapi.Header, err error,
) {
	if o.streamParser != nil {
		newHeaders = []internalapi.Header{{contentTypeHeaderName, eventStreamContentType}}
	}
	return
}

// ResponseBody implements [OpenAIChatCompletionTranslator.ResponseBody] for AWS Anthropic.
// AWS Anthropic uses deterministic model mapping without virtualization, where the requested model
// is exactly what gets executed. The response does not contain a model field, so we return
// the request model that was originally sent.
func (o *openAIToAWSAnthropicTranslatorV1ChatCompletion) ResponseBody(_ map[string]string, body io.Reader, endOfStream bool, span tracingapi.ChatCompletionSpan) (
	newHeaders []internalapi.Header, newBody []byte, tokenUsage usage.TokenUsage, responseModel string, err error,
) {
	// If a stream parser was initialized, this is a streaming request.
	if o.streamParser != nil {
		defer func() { tokenUsage = o.usageDecoder.Snapshot() }()
		defer func() {
			if err == nil && endOfStream && !o.amazonState.terminal {
				err = ErrMissingTerminal
			}
		}()
		var anthropicSSE []byte
		err = o.amazonState.read(body, &o.bufferedBody, o.streamLimit, endOfStream, func(msg eventstream.Message) error {
			data, raw, decodeErr := unwrapAnthropicEvent(msg)
			if decodeErr != nil {
				return decodeErr
			}
			o.usageDecoder.Schema = "AWSAnthropic"
			if gjson.ValidBytes(raw) {
				if _, observeErr := o.usageDecoder.Observe(raw); observeErr != nil {
					return observeErr
				}
			}
			anthropicSSE = append(anthropicSSE, data...)
			return nil
		})
		if err != nil {
			return nil, nil, tokenUsage, responseModel, err
		}

		// Pass the extracted SSE to the Anthropic parser
		return o.streamParser.Process(bytes.NewReader(anthropicSSE), endOfStream, span)
	}

	var anthropicResp anthropic.Message
	if err = json.NewDecoder(body).Decode(&anthropicResp); err != nil {
		return nil, nil, tokenUsage, "", fmt.Errorf("failed to unmarshal body: %w", err)
	}

	responseModel = o.requestModel
	if anthropicResp.Model != "" {
		responseModel = anthropicResp.Model
	}

	openAIResp, tokenUsage, err := messageToChatCompletion(&anthropicResp, responseModel)
	if err != nil {
		return nil, nil, usage.TokenUsage{}, "", err
	}

	newBody, err = json.Marshal(openAIResp)
	if err != nil {
		return nil, nil, usage.TokenUsage{}, "", fmt.Errorf("failed to marshal body: %w", err)
	}

	if span != nil {
		span.RecordResponse(openAIResp)
	}
	newHeaders = []internalapi.Header{{contentLengthHeaderName, strconv.Itoa(len(newBody))}}
	return
}

func unwrapAnthropicEvent(msg eventstream.Message) ([]byte, []byte, error) {
	var payload struct {
		Bytes []byte `json:"bytes"`
	}
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return nil, nil, fmt.Errorf("decode anthropic envelope: %w", err)
	}
	if len(payload.Bytes) == 0 {
		return nil, nil, errors.New("missing anthropic event payload")
	}
	eventType := gjson.GetBytes(payload.Bytes, "type").String()
	return fmt.Appendf(nil, "event: %s\ndata: %s\n\n", eventType, payload.Bytes), payload.Bytes, nil
}

// SetStreamLimit sets the maximum retained binary frame size in bytes.
func (o *openAIToAWSAnthropicTranslatorV1ChatCompletion) SetStreamLimit(limit int) {
	o.streamLimit = limit
}

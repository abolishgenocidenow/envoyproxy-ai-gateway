// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
package main

import (
	"github.com/envoyproxy/ai-gateway/cmd/controller/mainlib"
	"os"
	ctrl "sigs.k8s.io/controller-runtime"
)

func main() {
	if err := mainlib.Main(ctrl.SetupSignalHandler(), os.Args[1:], mainlib.Options{}); err != nil {
		ctrl.Log.WithName("setup").Error(err, "controller stopped")
		os.Exit(1)
	}
}

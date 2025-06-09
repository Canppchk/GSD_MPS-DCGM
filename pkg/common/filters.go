package common

// SkipNodes holds nodes you never want to schedule on.
// Expand via ConfigMap/env in production.
var SkipNodes = map[string]struct{}{
	// "gsd-ctrlr": {}, // example
}

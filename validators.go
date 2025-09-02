package rmqkit

import (
	"errors"
	"io"
	"reflect"
	"syscall"
)

// validateFields returns missing and wrong type fields found in args.
// requiredFields is a map of field names with their expected types.
func validateFields(args map[string]any, requiredFields map[string]any) ([]string, []string) {
	var missing []string
	var wrongType []string

	for field, expectedVal := range requiredFields {
		val, exists := args[field]
		if !exists {
			missing = append(missing, field)
			continue
		}

		expectedReflect := reflect.TypeOf(expectedVal)
		valueReflect := reflect.TypeOf(val)

		// Default type switch will end up with false positive results.
		// E.g., 123.(string) -> ok.
		if expectedReflect != valueReflect {
			wrongType = append(wrongType, field)
		}
	}

	return missing, wrongType
}

// isRetryable returns true if the error can be retried.
// Any connection/timout error is considered retryable.
func isRetryable(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, errTimeoutExceeded)
}

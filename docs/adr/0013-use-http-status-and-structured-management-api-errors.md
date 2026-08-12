# Use HTTP status and structured management API errors

Management API failures will use meaningful HTTP status codes and a JSON error object containing a stable machine-readable code, a human-readable message, and a request identifier when available. Successful responses will return their resource directly rather than a generic success wrapper, and partial failures must remain visible to the affected console module instead of being represented as empty data.

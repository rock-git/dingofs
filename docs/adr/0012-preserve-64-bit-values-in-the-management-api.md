# Preserve 64-bit values in the management API

The management API will encode identifiers and other exact 64-bit integers as decimal strings so JavaScript cannot silently round them. Small bounded counts, enums, and pagination values may remain JSON numbers; timestamps will use ISO 8601 UTC strings, and capacities or durations will be returned in exact base units rather than preformatted display text, with presentation-unit conversion owned by the console.

# Isolate the console security policy from legacy pages

The React console will ship with a restrictive Content Security Policy that permits scripts and network requests only from the serving MDS, along with `nosniff`, no-referrer, and anti-framing headers. Legacy pages will retain a separate policy while they still require inline scripts; their requirements must not weaken the new console, although inline styles may remain allowed where shadcn/Radix positioning requires them.

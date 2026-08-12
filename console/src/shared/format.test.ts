import { describe, expect, it } from 'vitest';
import { formatBytes, formatTime, toneForState } from './format';

describe('console formatting', () => {
  it('uses IEC units without converting exact identifiers', () => {
    expect(formatBytes('1048576')).toBe('1.00 MiB');
    expect(formatBytes('18446744073709551600')).toBe('18446744073709551600 bytes');
  });

  it('formats timestamps with a local timezone without throwing', () => {
    expect(formatTime('2026-08-12T08:30:00.000Z')).not.toBe('Invalid timestamp');
  });

  it('keeps resource-native state colors predictable', () => {
    expect(toneForState('ONLINE')).toBe('good');
    expect(toneForState('UNSTABLE')).toBe('warn');
    expect(toneForState('OFFLINE')).toBe('bad');
  });
});

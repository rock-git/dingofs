export function formatBytes(value: string | undefined): string {
  if (!value) return '—';
  const bytes = Number(value);
  if (!Number.isSafeInteger(bytes)) return `${value} bytes`;
  const units = ['bytes', 'KiB', 'MiB', 'GiB', 'TiB'];
  let amount = bytes;
  let index = 0;
  while (amount >= 1024 && index < units.length - 1) { amount /= 1024; index += 1; }
  return `${amount.toFixed(index === 0 ? 0 : 2)} ${units[index]}`;
}

export function formatTime(value: string | undefined): string {
  if (!value) return '—';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return 'Invalid timestamp';
  return new Intl.DateTimeFormat(undefined, {
    year: 'numeric', month: 'short', day: 'numeric',
    hour: '2-digit', minute: '2-digit', timeZoneName: 'short',
  }).format(date);
}

export function toneForState(state: string): 'neutral' | 'good' | 'warn' | 'bad' | 'info' {
  const normalized = state.toLowerCase();
  if (['normal', 'online', 'healthy', 'init'].includes(normalized)) return 'good';
  if (['unstable', 'recycling', 'degraded'].includes(normalized)) return 'warn';
  if (['offline', 'abnormal', 'deleted', 'unhealthy'].includes(normalized)) return 'bad';
  return 'neutral';
}

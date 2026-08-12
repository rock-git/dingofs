import { useEffect, useState } from 'react';

type Theme = 'light' | 'dark' | 'system';
const key = 'dingofs-console-theme';

function applyTheme(theme: Theme) {
  const dark = theme === 'dark' || (theme === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches);
  document.documentElement.dataset.theme = dark ? 'dark' : 'light';
}

export function useTheme() {
  const [theme, setThemeState] = useState<Theme>(() => (localStorage.getItem(key) as Theme | null) ?? 'system');
  useEffect(() => { applyTheme(theme); localStorage.setItem(key, theme); }, [theme]);
  useEffect(() => {
    if (theme !== 'system') return;
    const media = window.matchMedia('(prefers-color-scheme: dark)');
    const listener = () => applyTheme('system');
    media.addEventListener('change', listener);
    return () => media.removeEventListener('change', listener);
  }, [theme]);
  return { theme, setTheme: setThemeState };
}

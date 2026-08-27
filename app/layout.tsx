import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'ReleaseGuard — Real-time canary safety',
  description: 'Ship faster with a streaming safety net powered by Confluent.',
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}

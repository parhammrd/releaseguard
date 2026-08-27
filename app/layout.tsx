import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'ReleaseGuard — Real-time canary safety',
  description: 'Canary-release monitoring demo built with Confluent and Flink.',
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}

import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "KKBox Realtime BI",
  description: "Realtime BI dashboard powered by FastAPI + Next.js"
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}


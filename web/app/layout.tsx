import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "DexScanner — Solana Dip Scanner",
  description: "Dashboard & Launcher for DexScreener Dip Scanner",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="dark">
      <body className="min-h-screen bg-background font-sans">{children}</body>
    </html>
  );
}

import type React from "react"
import type { Metadata } from "next"
import { Geist, Geist_Mono } from "next/font/google"
import "./globals.css"
import { RootLayoutWrapper } from "@/components/layout/root-layout-wrapper"
import { ReduxProvider } from "@/components/providers/ReduxProvider"
import { Toaster } from "@/components/ui/toaster"

const _geist = Geist({ subsets: ["latin"] })
const _geistMono = Geist_Mono({ subsets: ["latin"] })

export const metadata: Metadata = {
  title: "CDC Replication Admin",
  description: "Enterprise-grade Change Data Capture platform for real-time data replication and monitoring",
}

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode
}>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        <script
          dangerouslySetInnerHTML={{
            __html: `
              try {
                const stored = localStorage.getItem('theme');
                const theme = stored || 'dark';
                document.documentElement.classList.toggle('dark', theme === 'dark');
                document.documentElement.classList.toggle('light', theme === 'light');
              } catch (e) {}
            `,
          }}
        />
      </head>
      <body className={`font-sans antialiased bg-background text-foreground`} suppressHydrationWarning>
        <ReduxProvider>
          <RootLayoutWrapper>{children}</RootLayoutWrapper>
          <Toaster />
        </ReduxProvider>
      </body>
    </html>
  )
}

"use client"

import { ReactNode } from "react"
import { LucideIcon } from "lucide-react"

interface PageHeaderProps {
  title: string
  subtitle: string
  icon: LucideIcon
  action?: ReactNode
}

export function PageHeader({ title, subtitle, icon: Icon, action }: PageHeaderProps) {
  return (
    <div className="flex items-center justify-between pb-6 border-b border-border mb-6">
      <div className="flex flex-col gap-1">
        <h1 className="text-2xl font-bold text-foreground flex items-center gap-2 tracking-tight">
          <div className="p-1.5 bg-primary/10 rounded-lg">
            <Icon className="w-5 h-5 text-primary" />
          </div>
          {title}
        </h1>
        <p className="text-sm text-foreground-muted ml-1">{subtitle}</p>
      </div>
      {action && <div className="flex items-center gap-2">{action}</div>}
    </div>
  )
}


"use client"

import * as React from "react"
import { Grid, List } from "lucide-react"
import { Button } from "@/components/ui/button"

interface ViewToggleProps {
    view: "grid" | "list"
    onViewChange: (view: "grid" | "list") => void
}

export function ViewToggle({ view, onViewChange }: ViewToggleProps) {
    return (
        <div className="flex items-center gap-1 p-1 bg-muted rounded-lg border border-border">
            <Button
                variant={view === "grid" ? "default" : "ghost"}
                size="icon"
                className={`h-7 w-7 ${view === "grid" ? "bg-white text-primary shadow-sm hover:bg-white/90 dark:bg-zinc-800" : "text-muted-foreground hover:text-foreground"}`}
                onClick={() => onViewChange("grid")}
                title="Grid View"
            >
                <Grid className="h-4 w-4" />
            </Button>
            <Button
                variant={view === "list" ? "default" : "ghost"}
                size="icon"
                className={`h-7 w-7 ${view === "list" ? "bg-white text-primary shadow-sm hover:bg-white/90 dark:bg-zinc-800" : "text-muted-foreground hover:text-foreground"}`}
                onClick={() => onViewChange("list")}
                title="List View"
            >
                <List className="h-4 w-4" />
            </Button>
        </div>
    )
}

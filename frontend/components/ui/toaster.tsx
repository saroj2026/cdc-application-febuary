"use client"

import { useToast } from "./use-toast"
import { X } from "lucide-react"

export function Toaster() {
    const { toasts } = useToast()

    return (
        <div className="fixed top-0 right-0 z-[100] flex max-h-screen w-full flex-col-reverse p-4 sm:top-auto sm:right-0 sm:bottom-0 sm:flex-col md:max-w-[420px] pointer-events-none">
            {toasts.map(function ({ id, title, description, variant, ...props }) {
                return (
                    <div
                        key={id}
                        className="pointer-events-auto mb-4 flex w-full flex-col items-start gap-2 overflow-hidden rounded-lg border p-4 shadow-lg transition-all data-[swipe=cancel]:translate-x-0 data-[swipe=end]:translate-x-[var(--radix-toast-swipe-end-x)] data-[swipe=move]:translate-x-[var(--radix-toast-swipe-move-x)] data-[swipe=move]:transition-none data-[state=open]:animate-in data-[state=closed]:animate-out data-[swipe=end]:animate-out data-[state=closed]:fade-out-80 data-[state=closed]:slide-out-to-right-full data-[state=open]:slide-in-from-top-full data-[state=open]:sm:slide-in-from-bottom-full"
                        style={{
                            backgroundColor: variant === "destructive" ? "rgba(239, 68, 68, 0.1)" : "rgba(6, 182, 212, 0.1)",
                            borderColor: variant === "destructive" ? "rgba(239, 68, 68, 0.3)" : "rgba(6, 182, 212, 0.3)",
                        }}
                    >
                        <div className="grid gap-1 flex-1">
                            {title && (
                                <div className="text-sm font-semibold" style={{ color: variant === "destructive" ? "#ef4444" : "#06b6d4" }}>
                                    {title}
                                </div>
                            )}
                            {description && (
                                <div className="text-sm opacity-90 text-foreground-muted">
                                    {description}
                                </div>
                            )}
                        </div>
                        <button
                            onClick={() => {
                                // Auto-dismiss after showing
                            }}
                            className="absolute right-2 top-2 rounded-md p-1 opacity-70 transition-opacity hover:opacity-100"
                        >
                            <X className="h-4 w-4" />
                        </button>
                    </div>
                )
            })}
        </div>
    )
}

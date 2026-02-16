"use client"

import { useState, useEffect } from "react"
import { AlertCircle, CheckCircle, Info, XCircle } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"

interface ConfirmDialogProps {
    isOpen: boolean
    onClose: () => void
    onConfirm: () => void
    title: string
    message: string
    confirmText?: string
    cancelText?: string
    variant?: "danger" | "warning" | "info" | "success"
    hideCancel?: boolean
}

export function ConfirmDialog({
    isOpen,
    onClose,
    onConfirm,
    title,
    message,
    confirmText = "Confirm",
    cancelText = "Cancel",
    variant = "warning",
    hideCancel = false
}: ConfirmDialogProps) {
    const [mounted, setMounted] = useState(false)

    useEffect(() => {
        setMounted(true)
    }, [])

    if (!mounted || !isOpen) return null

    const variantStyles = {
        danger: {
            gradient: "from-red-500/10 to-red-600/5",
            border: "border-red-500/30",
            icon: <AlertCircle className="w-12 h-12 text-red-400" />,
            confirmBg: "bg-red-500 hover:bg-red-600",
            iconBg: "bg-red-500/10"
        },
        warning: {
            gradient: "from-amber-500/10 to-amber-600/5",
            border: "border-amber-500/30",
            icon: <AlertCircle className="w-12 h-12 text-amber-400" />,
            confirmBg: "bg-amber-500 hover:bg-amber-600",
            iconBg: "bg-amber-500/10"
        },
        info: {
            gradient: "from-blue-500/10 to-blue-600/5",
            border: "border-blue-500/30",
            icon: <Info className="w-12 h-12 text-blue-400" />,
            confirmBg: "bg-blue-500 hover:bg-blue-600",
            iconBg: "bg-blue-500/10"
        },
        success: {
            gradient: "from-green-500/10 to-green-600/5",
            border: "border-green-500/30",
            icon: <CheckCircle className="w-12 h-12 text-green-400" />,
            confirmBg: "bg-green-500 hover:bg-green-600",
            iconBg: "bg-green-500/10"
        }
    }

    const style = variantStyles[variant]

    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm">
            <Card
                className={`max-w-md w-full bg-gradient-to-br ${style.gradient} border-2 ${style.border} shadow-2xl`}
                onClick={(e) => e.stopPropagation()}
            >
                <div className="p-6">
                    {/* Icon */}
                    <div className={`w-16 h-16 rounded-full ${style.iconBg} flex items-center justify-center mx-auto mb-4`}>
                        {style.icon}
                    </div>

                    {/* Title */}
                    <h2 className="text-2xl font-bold text-foreground text-center mb-3">
                        {title}
                    </h2>

                    {/* Message */}
                    <p className="text-foreground-muted text-center mb-6 leading-relaxed">
                        {message}
                    </p>

                    {/* Actions */}
                    <div className="flex gap-3">
                        {!hideCancel && (
                            <Button
                                variant="outline"
                                onClick={onClose}
                                className="flex-1 bg-transparent border-border hover:bg-surface-hover"
                            >
                                {cancelText}
                            </Button>
                        )}
                        <Button
                            onClick={() => {
                                onConfirm()
                                onClose()
                            }}
                            className={`flex-1 ${style.confirmBg} text-white shadow-lg hover:shadow-xl`}
                        >
                            {confirmText}
                        </Button>
                    </div>
                </div>
            </Card>
        </div>
    )
}

// Hook for easier usage
export function useConfirmDialog() {
    const [isOpen, setIsOpen] = useState(false)
    const [config, setConfig] = useState<Omit<ConfirmDialogProps, "isOpen" | "onClose" | "onConfirm">>({
        title: "",
        message: "",
        variant: "warning",
        hideCancel: false
    })
    const [onConfirmCallback, setOnConfirmCallback] = useState<(() => void) | null>(null)

    const showConfirm = (
        title: string,
        message: string,
        onConfirm: () => void,
        variant: "danger" | "warning" | "info" | "success" = "warning",
        confirmText?: string,
        cancelText?: string,
        hideCancel: boolean = false
    ) => {
        setConfig({ title, message, variant, confirmText, cancelText, hideCancel })
        setOnConfirmCallback(() => onConfirm)
        setIsOpen(true)
    }

    const ConfirmDialogComponent = () => (
        <ConfirmDialog
            isOpen={isOpen}
            onClose={() => setIsOpen(false)}
            onConfirm={() => {
                if (onConfirmCallback) {
                    onConfirmCallback()
                }
            }}
            {...config}
        />
    )

    return { showConfirm, ConfirmDialogComponent }
}

"use client"
import { PipelineWizard } from "./pipeline-wizard"

interface PipelineModalProps {
  isOpen: boolean
  onClose: () => void
  onSave: (data: any) => void
}

const mockTables = {
  mysql: ["orders", "order_items", "customers", "products", "inventory"],
  postgresql: ["users", "transactions", "analytics", "events"],
}

export function PipelineModal({ isOpen, onClose, onSave }: PipelineModalProps) {
  return <PipelineWizard isOpen={isOpen} onClose={onClose} onSave={onSave} />
}

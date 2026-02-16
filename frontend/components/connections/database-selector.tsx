"use client"

import { useState, useMemo } from "react"
import { Card } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { DATABASE_SERVICES, DatabaseInfo } from "@/lib/database-icons"
import { Search, ChevronRight, Database, Sparkles, Zap } from "lucide-react"
import { DatabaseLogo } from "@/lib/database-logo-loader"

interface DatabaseSelectorProps {
  onSelect: (database: DatabaseInfo) => void
  onCancel: () => void
}

export function DatabaseSelector({ onSelect, onCancel }: DatabaseSelectorProps) {
  const [searchQuery, setSearchQuery] = useState("")
  const [selectedCategory, setSelectedCategory] = useState<string | null>(null)
  const [hoveredId, setHoveredId] = useState<string | null>(null)

  // Filter databases based on search and category
  const filteredDatabases = useMemo(() => {
    let filtered = DATABASE_SERVICES

    // Filter by category
    if (selectedCategory) {
      filtered = filtered.filter(db => db.category === selectedCategory)
    }

    // Filter by search query
    if (searchQuery.trim()) {
      const query = searchQuery.toLowerCase()
      filtered = filtered.filter(db =>
        db.name.toLowerCase().includes(query) ||
        db.displayName.toLowerCase().includes(query) ||
        db.connectionType.toLowerCase().includes(query)
      )
    }

    return filtered
  }, [searchQuery, selectedCategory])

  // Get unique categories
  const categories = useMemo(() => {
    const cats = new Set(DATABASE_SERVICES.map(db => db.category))
    return Array.from(cats)
  }, [])

  const categoryLabels: Record<string, string> = {
    relational: 'Relational',
    nosql: 'NoSQL',
    warehouse: 'Data Warehouse',
    cloud: 'Cloud Services',
    other: 'Other'
  }

  const categoryIcons: Record<string, string> = {
    relational: '🗄️',
    nosql: '📊',
    warehouse: '☁️',
    cloud: '🌐',
    other: '🔧'
  }

  return (
    <div className="space-y-6 animate-fade-in">
      {/* Progress Bar with Animation */}
      <div className="flex items-center gap-3">
        <div className="flex-1 h-3 bg-surface-hover rounded-full overflow-hidden shadow-inner border border-border/50">
          <div className="h-full bg-gradient-to-r from-primary via-primary/90 to-primary rounded-full shadow-lg progress-bar-animate" />
        </div>
        <div className="px-3 py-1.5 bg-gradient-to-r from-primary/10 to-primary/5 rounded-lg border border-primary/20">
          <span className="text-xs font-semibold text-primary">Step 1 of 2</span>
        </div>
      </div>

      {/* Header with Gradient */}
      <div className="space-y-2">
        <div className="flex items-center gap-3">
          <div className="p-3 bg-gradient-to-br from-primary/20 via-primary/15 to-primary/10 rounded-2xl border border-primary/20 shadow-lg shadow-primary/10">
            <Sparkles className="w-6 h-6 text-primary animate-pulse" />
          </div>
          <div>
            <h2 className="text-4xl font-bold bg-gradient-to-r from-foreground via-foreground/90 to-foreground/70 bg-clip-text text-transparent">
              Database Services
            </h2>
            <p className="text-foreground-muted mt-1.5 text-base">Choose your database service to get started</p>
          </div>
        </div>
      </div>

      {/* Search Bar with Enhanced Design */}
      <div className="relative group">
        <div className="absolute left-4 top-1/2 -translate-y-1/2 z-10">
          <Search className="w-5 h-5 text-foreground-muted group-focus-within:text-primary transition-colors duration-200" />
        </div>
        <Input
          placeholder="Search for Connector... (e.g., MySQL, PostgreSQL, MongoDB)"
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          className="pl-12 pr-10 h-14 bg-surface/50 backdrop-blur-sm border-2 border-border focus:border-primary focus:ring-4 focus:ring-primary/20 text-foreground text-base transition-all duration-300 shadow-lg hover:shadow-xl"
        />
        {searchQuery && (
          <button
            onClick={() => setSearchQuery("")}
            className="absolute right-4 top-1/2 -translate-y-1/2 text-foreground-muted hover:text-foreground transition-colors p-1 hover:bg-surface-hover rounded-full"
          >
            ✕
          </button>
        )}
        {filteredDatabases.length > 0 && searchQuery && (
          <div className="absolute right-4 top-1/2 -translate-y-1/2 text-xs text-foreground-muted bg-surface px-2 py-1 rounded border border-border">
            {filteredDatabases.length} found
          </div>
        )}
      </div>

      {/* Category Filters with Enhanced Design */}
      <div className="flex flex-wrap gap-3">
        <button
          onClick={() => setSelectedCategory(null)}
          className={`px-5 py-2.5 rounded-xl font-semibold text-sm transition-all duration-300 transform hover:scale-105 active:scale-95 ${
            selectedCategory === null
              ? "bg-gradient-to-r from-primary via-primary/90 to-primary text-white shadow-xl shadow-primary/30 scale-105"
              : "bg-surface border-2 border-border text-foreground-muted hover:border-primary/50 hover:text-foreground hover:bg-surface-hover shadow-md hover:shadow-lg"
          }`}
        >
          <span className="flex items-center gap-2">
            <Zap className="w-4 h-4" />
            All
            <span className="px-2 py-0.5 bg-white/20 rounded-full text-xs">
              {DATABASE_SERVICES.length}
            </span>
          </span>
        </button>
        {categories.map((category) => (
          <button
            key={category}
            onClick={() => setSelectedCategory(category)}
            className={`px-5 py-2.5 rounded-xl font-semibold text-sm transition-all duration-300 transform hover:scale-105 active:scale-95 flex items-center gap-2 ${
              selectedCategory === category
                ? "bg-gradient-to-r from-primary via-primary/90 to-primary text-white shadow-xl shadow-primary/30 scale-105"
                : "bg-surface border-2 border-border text-foreground-muted hover:border-primary/50 hover:text-foreground hover:bg-surface-hover shadow-md hover:shadow-lg"
            }`}
          >
            <span className="text-lg">{categoryIcons[category]}</span>
            <span>{categoryLabels[category] || category}</span>
            <span className={`px-2 py-0.5 rounded-full text-xs ${
              selectedCategory === category ? "bg-white/20" : "bg-surface-hover"
            }`}>
              {DATABASE_SERVICES.filter(db => db.category === category).length}
            </span>
          </button>
        ))}
      </div>

      {/* Database Grid with Staggered Animation */}
      {filteredDatabases.length > 0 ? (
        <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-6 gap-5 max-h-[60vh] overflow-y-auto pr-2 custom-scrollbar">
          {filteredDatabases.map((database, index) => {
            const isHovered = hoveredId === database.id
            return (
              <div
                key={database.id}
                className="database-card-wrapper"
                style={{ animationDelay: `${index * 0.03}s` }}
                onMouseEnter={() => setHoveredId(database.id)}
                onMouseLeave={() => setHoveredId(null)}
              >
                <Card
                  className="p-6 cursor-pointer border-2 transition-all duration-500 bg-gradient-to-br from-surface via-surface to-surface-hover hover:from-primary/10 hover:via-primary/15 hover:to-primary/5 border-border hover:border-primary/60 hover:shadow-2xl hover:shadow-primary/30 relative overflow-hidden group database-card"
                  onClick={() => onSelect(database)}
                >
                  {/* Animated Background Gradient */}
                  <div className="absolute inset-0 bg-gradient-to-br from-primary/0 via-primary/0 to-primary/0 opacity-0 group-hover:opacity-100 transition-opacity duration-500" />

                  {/* Shine Effect on Hover */}
                  <div className="absolute inset-0 overflow-hidden">
                    <div className="absolute inset-0 -translate-x-full group-hover:translate-x-full transition-transform duration-1000 ease-in-out bg-gradient-to-r from-transparent via-white/20 to-transparent" />
                  </div>

                  {/* Glow Effect */}
                  <div className="absolute -inset-1 bg-gradient-to-r from-primary/0 via-primary/0 to-primary/0 opacity-0 group-hover:opacity-50 blur-xl transition-opacity duration-500" />

                  <div className="flex flex-col items-center text-center space-y-4 relative z-10">
                    {/* Icon with Enhanced Animation */}
                    <div className="relative">
                      <div className="w-20 h-20 rounded-2xl bg-gradient-to-br from-primary/25 via-primary/20 to-primary/15 flex items-center justify-center group-hover:from-primary/35 group-hover:via-primary/30 group-hover:to-primary/25 transition-all duration-500 shadow-xl group-hover:shadow-2xl group-hover:shadow-primary/40 group-hover:scale-110 icon-container">
                        <DatabaseLogo
                          connectionType={database.connectionType}
                          databaseId={database.id}
                          displayName={database.displayName}
                          size={48}
                          className="w-12 h-12 icon-logo"
                        />
                      </div>
                      
                      {/* Beta Badge */}
                      {database.beta && (
                        <div className="absolute -top-2 -right-2 px-2.5 py-1 bg-gradient-to-r from-warning to-warning/80 text-warning-foreground text-[10px] font-bold rounded-full shadow-lg border border-warning/30 animate-bounce-subtle">
                          Beta
                        </div>
                      )}

                      {/* Pulse Rings on Hover */}
                      {isHovered && (
                        <>
                          <div className="absolute inset-0 rounded-2xl border-2 border-primary/50 animate-pulse-ring-1" />
                          <div className="absolute inset-0 rounded-2xl border-2 border-primary/30 animate-pulse-ring-2" />
                        </>
                      )}
                    </div>

                    {/* Name with Animation */}
                    <div className="w-full space-y-1">
                      <p className="text-sm font-bold text-foreground group-hover:text-primary transition-colors duration-300 truncate">
                        {database.displayName}
                      </p>
                      <p className="text-xs text-foreground-muted opacity-0 group-hover:opacity-100 transition-opacity duration-300 truncate">
                        {database.connectionType}
                      </p>
                    </div>

                    {/* Arrow Indicator */}
                    <div className="absolute top-4 right-4 opacity-0 group-hover:opacity-100 transition-all duration-300 transform translate-x-2 group-hover:translate-x-0">
                      <ChevronRight className="w-5 h-5 text-primary" />
                    </div>
                  </div>
                </Card>
              </div>
            )
          })}
        </div>
      ) : (
        <div className="text-center py-20">
          <div className="inline-block mb-6 animate-bounce-subtle">
            <Database className="w-20 h-20 mx-auto text-foreground-muted" />
          </div>
          <p className="text-xl font-bold text-foreground mb-2">No databases found</p>
          <p className="text-foreground-muted mb-4">Try adjusting your search or filter criteria</p>
          <Button
            variant="outline"
            onClick={() => {
              setSearchQuery("")
              setSelectedCategory(null)
            }}
            className="mt-4"
          >
            Clear Filters
          </Button>
        </div>
      )}

      {/* Footer Actions */}
      <div className="flex justify-end gap-3 pt-6 border-t border-border">
        <Button
          variant="outline"
          onClick={onCancel}
          className="border-border hover:bg-surface-hover px-6 py-2.5 transition-all duration-200 hover:scale-105 active:scale-95"
        >
          Cancel
        </Button>
        {filteredDatabases.length === 1 && (
          <Button
            onClick={() => onSelect(filteredDatabases[0])}
            className="bg-gradient-to-r from-primary to-primary/90 hover:from-primary/90 hover:to-primary text-white px-6 py-2.5 shadow-xl shadow-primary/30 transition-all duration-200 hover:scale-105 active:scale-95"
          >
            Select {filteredDatabases[0].displayName}
          </Button>
        )}
      </div>

      <style jsx global>{`
        @keyframes fade-in {
          from {
            opacity: 0;
            transform: translateY(10px);
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }

        @keyframes progress-bar {
          from {
            width: 0%;
          }
          to {
            width: 25%;
          }
        }

        @keyframes slide-up {
          from {
            opacity: 0;
            transform: translateY(20px) scale(0.95);
          }
          to {
            opacity: 1;
            transform: translateY(0) scale(1);
          }
        }

        @keyframes pulse-ring-1 {
          0%, 100% {
            transform: scale(1);
            opacity: 0.5;
          }
          50% {
            transform: scale(1.15);
            opacity: 0;
          }
        }

        @keyframes pulse-ring-2 {
          0%, 100% {
            transform: scale(1);
            opacity: 0.3;
          }
          50% {
            transform: scale(1.25);
            opacity: 0;
          }
        }

        @keyframes bounce-subtle {
          0%, 100% {
            transform: translateY(0);
          }
          50% {
            transform: translateY(-5px);
          }
        }

        .animate-fade-in {
          animation: fade-in 0.4s ease-out;
        }

        .progress-bar-animate {
          animation: progress-bar 0.8s ease-out forwards;
        }

        .database-card-wrapper {
          animation: slide-up 0.5s ease-out backwards;
        }

        .database-card {
          transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
        }

        .database-card:hover {
          transform: translateY(-8px) scale(1.02);
        }

        .icon-container {
          transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
        }

        .database-card:hover .icon-container {
          transform: scale(1.1) rotate(5deg);
        }

        .icon-logo {
          transition: transform 0.3s ease-out;
        }

        .database-card:hover .icon-logo {
          transform: scale(1.1);
        }

        .animate-pulse-ring-1 {
          animation: pulse-ring-1 2s ease-in-out infinite;
        }

        .animate-pulse-ring-2 {
          animation: pulse-ring-2 2s ease-in-out infinite 0.5s;
        }

        .animate-bounce-subtle {
          animation: bounce-subtle 2s ease-in-out infinite;
        }

        .custom-scrollbar::-webkit-scrollbar {
          width: 10px;
        }

        .custom-scrollbar::-webkit-scrollbar-track {
          background: var(--surface-hover);
          border-radius: 5px;
        }

        .custom-scrollbar::-webkit-scrollbar-thumb {
          background: linear-gradient(to bottom, var(--primary), var(--primary) / 0.8);
          border-radius: 5px;
          border: 2px solid var(--surface-hover);
        }

        .custom-scrollbar::-webkit-scrollbar-thumb:hover {
          background: var(--primary);
        }
      `}</style>
    </div>
  )
}

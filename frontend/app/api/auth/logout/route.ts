import { type NextRequest, NextResponse } from "next/server"

export async function POST(request: NextRequest) {
  try {
    // Clear user session
    const response = NextResponse.json({ message: "Logged out successfully" })
    response.cookies.delete("session")
    return response
  } catch (error) {
    console.error("[v0] Logout error:", error)
    return NextResponse.json({ message: "Internal server error" }, { status: 500 })
  }
}

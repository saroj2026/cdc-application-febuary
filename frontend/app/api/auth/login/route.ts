import { type NextRequest, NextResponse } from "next/server"

export async function POST(request: NextRequest) {
  try {
    const { email, password } = await request.json()

    // Validate input
    if (!email || !password) {
      return NextResponse.json({ message: "Email and password are required" }, { status: 400 })
    }

    // Demo authentication (replace with real database)
    if (email === "demo@example.com" && password === "Demo@1234") {
      return NextResponse.json({
        user: {
          id: "user_1",
          email: "demo@example.com",
          fullName: "Demo User",
          role: "admin",
        },
      })
    }

    return NextResponse.json({ message: "Invalid email or password" }, { status: 401 })
  } catch (error) {
    console.error("[v0] Login error:", error)
    return NextResponse.json({ message: "Internal server error" }, { status: 500 })
  }
}

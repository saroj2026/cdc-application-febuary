import { type NextRequest, NextResponse } from "next/server"

export async function POST(request: NextRequest) {
  try {
    const { fullName, email, password } = await request.json()

    // Validate input
    if (!fullName || !email || !password) {
      return NextResponse.json({ message: "All fields are required" }, { status: 400 })
    }

    if (password.length < 8) {
      return NextResponse.json({ message: "Password must be at least 8 characters" }, { status: 400 })
    }

    // In production, hash password with bcrypt and store in database
    // For now, just return success
    return NextResponse.json({
      user: {
        id: "user_" + Date.now(),
        email,
        fullName,
        role: "user",
      },
      message: "Account created successfully",
    })
  } catch (error) {
    console.error("[v0] Signup error:", error)
    return NextResponse.json({ message: "Internal server error" }, { status: 500 })
  }
}

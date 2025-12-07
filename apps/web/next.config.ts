import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  reactCompiler: true,
  async rewrites() {
    return [
      {
        source: "/api/:path*",
        destination: process.env.NODE_ENV === 'development'
          ? "http://127.0.0.1:8000/:path*" // Local Python
          : "https://your-render-app.com/:path*", // Prod Python
      },
    ];
  },
};

export default nextConfig;
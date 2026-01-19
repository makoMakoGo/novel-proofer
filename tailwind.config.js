/** @type {import("tailwindcss").Config} */
module.exports = {
  content: ["./templates/**/*.html", "./templates/static/js/**/*.js"],
  theme: {
    extend: {
      fontFamily: {
        sans: ['"Inter"', '"Noto Sans SC"', "sans-serif"],
        serif: ['"Noto Serif SC"', "serif"],
        mono: ['"JetBrains Mono"', '"SF Mono"', "Consolas", "monospace"],
      },
      colors: {
        paper: "#FDFBF7",
        ink: "#18181b",
      },
    },
  },
};

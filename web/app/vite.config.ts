import { defineConfig } from "vite";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL(".", import.meta.url));
const assetVersion = Date.now().toString(36);

export default defineConfig({
  root,
  base: "/",
  plugins: [{
    name: "embedded-cache-version",
    transformIndexHtml: {
      order: "post",
      handler(html) {
        return html
          .replace('src="/app.js"', `src="/app.js?v=${assetVersion}"`)
          .replace('href="/style.css"', `href="/style.css?v=${assetVersion}"`);
      },
    },
  }],
  build: {
    outDir: "../../embedded-dist",
    emptyOutDir: true,
    cssCodeSplit: false,
    minify: "esbuild",
    rollupOptions: {
      output: {
        entryFileNames: "app.js",
        assetFileNames: (asset) =>
          asset.names?.some((name) => name.endsWith(".css"))
            ? "style.css"
            : "[name][extname]",
        inlineDynamicImports: true,
      },
    },
  },
});

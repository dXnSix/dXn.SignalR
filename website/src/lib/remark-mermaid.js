// ```mermaid fences become a plain container that the browser renders into SVG
// (see the script in BaseLayout). Doing this before syntax highlighting keeps
// Shiki away from them; the escaped source stays readable if scripts are off.
const escapeHtml = (text) => text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

export function remarkMermaid() {
  return (tree) => {
    const walk = (node) => {
      if (!node.children) return;
      node.children = node.children.map((child) => {
        if (child.type === 'code' && child.lang === 'mermaid') {
          return {
            type: 'html',
            value: `<div class="mermaid-diagram" data-pagefind-ignore><div class="mermaid" role="img" aria-label="Diagram">${escapeHtml(child.value)}</div></div>`,
          };
        }
        walk(child);
        return child;
      });
    };
    walk(tree);
  };
}

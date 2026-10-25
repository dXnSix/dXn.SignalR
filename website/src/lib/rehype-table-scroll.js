import { visit } from 'unist-util-visit';

// A wide table (long signatures, code in cells) would stretch the page on a
// phone. Wrapping it gives the table its own horizontal scroll instead.
export function rehypeTableScroll() {
  return (tree) => {
    visit(tree, 'element', (node, index, parent) => {
      if (node.tagName !== 'table' || !parent || parent.properties?.className?.includes?.('table-scroll')) return;
      parent.children[index] = {
        type: 'element',
        tagName: 'div',
        properties: { className: ['table-scroll'], tabIndex: 0, role: 'region', ariaLabel: 'Scrollable table' },
        children: [node],
      };
      return 'skip';
    });
  };
}

import { button, div, ImportedStyleSheet, InlineStyleSheet, input, StyleSheetLike } from 'core/dom';
import type * as p from 'core/properties';
import { Dict } from 'core/types';
import { dict } from "core/util/object";
import { Widget, WidgetView } from "models/widgets/widget";
declare var $: any; // jQuery

interface JSTreeNode {
  id: string
  text: string
  type: string
  state: {
    opened: boolean
    disabled?: boolean
  },
  children: JSTreeNode[]
}

interface IconEntry {
  icon: string
}


export class TreeViewElementView extends WidgetView {
  declare model: TreeView

  private control_el: HTMLElement
  private search_el: HTMLInputElement
  private tree_el: HTMLElement

  private readonly icons_default: Map<string, string> = new Map([
    ['root', 'ti ti-database'],
    ['branch', 'ti ti-folder'],
    ['default', 'ti ti-file'],
  ]);

  override stylesheets(): StyleSheetLike[] {
    return [
      ...super.stylesheets(),
      new ImportedStyleSheet('https://cdnjs.cloudflare.com/ajax/libs/jstree/3.2.1/themes/default/style.min.css'),
      new ImportedStyleSheet(`https://cdn.jsdelivr.net/npm/@tabler/icons-webfont@latest/tabler-icons.min.css`),
      new InlineStyleSheet(`
.jstree-node {
    margin-left: 12px!important;
}
.jstree-node>.jstree-icon.jstree-ocl {
    width: 12px;
    background: none!important;
    transition: all 0.15s ease;
}
.jstree-node.jstree-open>.jstree-icon.jstree-ocl {
    transform: rotate(90deg);
}
.jstree-node.jstree-leaf>.jstree-icon.jstree-ocl {
    display: none;
}
.jstree-default .jstree-search {
  font-style: normal;
  font-size: 16px;
  font-weight: 700;
  color: #FFFFFF;
  background: #3cb371!important;
}
.jstree-default .jstree-wholerow-clicked {
  background: #90d5ff!important;
}
.bkext-treeview-btn {
  height: 24px;
  width: 24px;
  padding: 0;
  border: none;
  background: #fff;
  color: #5f5f5f;
  font-size: 20px;
  cursor: pointer;
}
.bkext-treeview-btn:hover {
  background: #f0f0f0;
}
`),
    ]
  }

  private _change_select_lock: boolean = false;
  private _change_select(callback: () => void): void {
    if (!this._change_select_lock) {
      this._change_select_lock = true;
      callback();
      this._change_select_lock = false;
    }
  }

  override connect_signals(): void {
    super.connect_signals()
    const { selected } = this.model.properties;
    this.on_change(selected, () => {
      this._change_select(() => this._select());
    });
  }

  override render(): void {
    super.render()

    // control panel
    this.control_el = div({
      style: {
        height: '24px',
        width: '100%',
        marginBottom: '2px'
      }
    });
    // expand button
    let expand_btn = button({
      class: 'bkext-treeview-btn ti ti-chevrons-down',
      title: 'Expand all',
    });
    $(expand_btn).on('click', () => {
      $(this.tree_el).jstree(true).open_all();
    });
    this.control_el.append(expand_btn);
    // collapse button
    let collapse_btn = button({
      class: 'bkext-treeview-btn ti ti-chevrons-up',
      title: 'Collapse all',
    });
    $(collapse_btn).on('click', () => {
      $(this.tree_el).jstree(true).close_all();
      $(this.tree_el).jstree(true).open_node('.');
    });
    this.control_el.append(collapse_btn);
    // select button
    let select_btn = button({
      class: 'bkext-treeview-btn ti ti-plus',
      title: 'Select all',
    });
    $(select_btn).on('click', () => {
      let tree = $(this.tree_el).jstree(true);
      for (const [path, _] of dict(this.model.paths)) {
        tree.select_node(path);
      }
    });
    this.control_el.append(select_btn);
    // deselect button
    let deselect_btn = button({
      class: 'bkext-treeview-btn ti ti-minus',
      title: 'Deselect all',
    });
    $(deselect_btn).on('click', () => {
      $(this.tree_el).jstree(true).deselect_all();
    });
    this.control_el.append(deselect_btn);
    // search
    this.search_el = input({
      style: {
        height: '24px',
        width: '100%',
        marginBottom: '2px'
      }
    });

    // tree
    this.tree_el = div({
      style: {
        height: 'calc(100% - 50px)',
        width: '100%',
        overflow: 'auto',
        border: '1px solid darkgray',
        borderRadius: '4px',
      }
    });
    $(this.tree_el).jstree({
      core: {
        data: this._build(),
        animation: false,
      },
      types: this._icon(),
      plugins: ['types', 'wholerow', 'sort', 'search', 'conditionalselect'],
      sort: function (a: any, b: any) {
        let leaf_a = this.is_leaf(a);
        let leaf_b = this.is_leaf(b);
        if ((leaf_a + leaf_b) === 1) {
          return leaf_a - leaf_b;
        }
        return this.get_text(a) > this.get_text(b) ? 1 : -1;
      },
      conditionalselect: function (node: any) {
        return this.is_leaf(node);
      },
    })
      .on('loaded.jstree after_open.jstree', () => {
        $(this.tree_el).find('.jstree-node>.jstree-icon.jstree-ocl').addClass('ti ti-caret-right');
      })
      .on('changed.jstree', (_: any, data: any) => {
        this._change_select(() => {
          this.model.selected = data.selected.slice();
        });
      });
    $(this.search_el).on('keyup', () => {
      $(this.tree_el).jstree(true).search($(this.search_el).val());
    });

    this.shadow_el.append(this.control_el);
    this.shadow_el.append(this.search_el);
    this.shadow_el.append(this.tree_el);
  }

  private _icon(): any {
    let types: Map<string, IconEntry> = new Map();
    for (const [type, icon] of this.icons_default) {
      types.set(type, { icon: icon });
    }
    for (const [type, icon] of dict(this.model.icons)) {
      types.set(type, { icon: icon });
    }
    return Object.fromEntries(types);
  }

  private _select(): void {
    const tree = $(this.tree_el).jstree(true);
    tree.deselect_all(true);
    tree.select_node(this.model.selected);
  }

  private _build(): JSTreeNode {
    const root: JSTreeNode = {
      id: '.',
      text: this.model.root,
      type: 'root',
      state: { opened: true, disabled: true },
      children: [],
    }
    let built: { [key: string]: JSTreeNode } = {};
    for (const [path, type] of dict(this.model.paths)) {
      let cur = root;
      const parts = path.split(this.model.separator);
      for (const part of parts) {
        let id = part;
        if (cur.type !== 'root') {
          id = cur.id + this.model.separator + part;
        }
        if (!(id in built)) {
          let node_type = 'branch';
          if (id === path) {
            node_type = type;
          }
          const node = {
            id: id,
            text: part,
            type: node_type,
            state: { opened: this.model.expand },
            children: [],
          }
          cur.children.push(node);
          built[id] = node;
        }
        cur = built[id];
      }
    }
    return root;
  }
}

export interface TreeView extends TreeView.Attrs { }

export namespace TreeView {
  export type Attrs = p.AttrsOf<Props>

  export type Props = Widget.Props & {
    root: p.Property<string>
    paths: p.Property<Dict<string>>
    separator: p.Property<string>
    expand: p.Property<boolean>
    icons: p.Property<Dict<string>>
    selected: p.Property<string[]>
  }
}

export class TreeView extends Widget {
  declare properties: TreeView.Props
  declare __view_type__: TreeViewElementView

  constructor(attrs?: Partial<TreeView.Attrs>) {
    super(attrs)
  }

  static {
    this.prototype.default_view = TreeViewElementView
    this.define<TreeView.Props>(({ Str, List, Bool, Dict }) => ({
      root: [Str, 'root'],
      paths: [Dict(Str), {}],
      separator: [Str, '/'],
      expand: [Bool, false],
      icons: [Dict(Str), {}],
      selected: [List(Str), []],
    }))
  }
}
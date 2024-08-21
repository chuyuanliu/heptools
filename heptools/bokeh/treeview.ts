import { div, ImportedStyleSheet, InlineStyleSheet, input, StyleSheetLike } from 'core/dom';
import * as p from 'core/properties';
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

  private search_el: HTMLInputElement
  private tree_el: HTMLElement

  private readonly icons_default: Map<string, string> = new Map([
    ['root', 'bi-database'],
    ['branch', 'bi-folder'],
    ['default', 'bi-file-earmark'],
  ]);

  override stylesheets(): StyleSheetLike[] {
    return [
      ...super.stylesheets(),
      new ImportedStyleSheet('https://cdnjs.cloudflare.com/ajax/libs/jstree/3.2.1/themes/default/style.min.css'),
      new ImportedStyleSheet('https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css'),
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
    const { selected_leaves } = this.model.properties;
    this.on_change(selected_leaves, () => {
      this._change_select(() => this._select());
    });
  }

  override render(): void {
    super.render()

    this.search_el = input({ style: { width: '100%', marginBottom: '5px' } });
    this.tree_el = div();
    $(this.tree_el).jstree({
      core: {
        data: this._build(),
        animation: false,
      },
      types: this._icon(),
      plugins: ['types', 'wholerow', 'sort', 'search', 'conditionalselect'],
      conditionalselect: function (node: any) { return this.is_leaf(node); },
    })
      .on('loaded.jstree after_open.jstree', () => {
        $(this.tree_el).find('.jstree-node>.jstree-icon.jstree-ocl').addClass('bi-caret-right');
      })
      .on('changed.jstree', (_: any, data: any) => {
        this._change_select(() => {
          this.model.selected_leaves = data.selected.slice();
        });
      });
    $(this.search_el).on('keyup', () => {
      $(this.tree_el).jstree(true).search($(this.search_el).val());
    });

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
    tree.select_node(this.model.selected_leaves);
  }

  private _build(): JSTreeNode {
    const root: JSTreeNode = {
      id: '',
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
        if (cur.id !== '') {
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
    selected_leaves: p.Property<string[]>
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
      selected_leaves: [List(Str), []],
    }))
  }
}
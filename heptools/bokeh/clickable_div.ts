import { ButtonClick } from "core/bokeh_events";
import { InlineStyleSheet, StyleSheetLike } from "core/dom";
import type * as p from "core/properties";
import type { EventCallback } from "model";
import { Div, DivView } from "models/widgets/div";

export class ClickableDivView extends DivView {
    declare model: ClickableDiv

    override stylesheets(): StyleSheetLike[] {
        return [
            ...super.stylesheets(),
            new InlineStyleSheet(`
div {
    cursor: pointer;
}
`),
        ]
    }

    override render(): void {
        super.render()

        this.markup_el.addEventListener("click", () => this.click())
    }

    click(): void {
        this.model.trigger_event(new ButtonClick())
    }
}

export interface ClickableDiv extends ClickableDiv.Attrs { }

export namespace ClickableDiv {
    export type Attrs = p.AttrsOf<Props>

    export type Props = Div.Props
}

export class ClickableDiv extends Div {
    declare properties: ClickableDiv.Props
    declare __view_type__: ClickableDivView

    constructor(attrs?: Partial<ClickableDiv.Attrs>) {
        super(attrs)
    }

    static {
        this.prototype.default_view = ClickableDivView
    }

    on_click(callback: EventCallback<ButtonClick>): void {
        this.on_event(ButtonClick, callback)
    }
}
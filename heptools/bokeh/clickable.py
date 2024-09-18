from bokeh.models import Div


class ClickableDiv(Div):
    __implementation__ = "clickable_div.ts"

from django import template

register = template.Library()

@register.filter
def get_model_fields(model):
    return [
        f.name for f in model._meta.get_fields()
        if not (f.many_to_many or f.one_to_many)
    ]

@register.filter
def col_label(value, labels_dict):
    return labels_dict.get(value, value)

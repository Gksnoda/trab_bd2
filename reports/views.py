# reports/views.py
from django.shortcuts import render
from django.apps import apps

def builder(request):
    # coleta todas as classes de modelo do app "reports"
    raw_models = apps.get_app_config('reports').get_models()

    # constrói lista de dicts sem usar _meta no template
    tables = [
        {
            'model': m,
            'name': m._meta.object_name,
            'table': m._meta.db_table,
        }
        for m in raw_models
    ]

    results = None
    if request.method == 'POST':
        # aqui vai a sua lógica de montar a query dinâmica...
        # por hora devolve um resultado de exemplo
        results = [
            {'foo': 'bar', 'baz': 123},
            {'foo': 'quux', 'baz': 456},
        ]

    return render(request, 'reports/builder.html', {
        'tables': tables,
        'results': results,
    })

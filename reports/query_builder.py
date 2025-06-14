from django.db.models import Q, Count, Sum, Avg, Max, Min
from .models import User, Game, Stream, Video, Clip

class DynamicQueryBuilder:
    AVAILABLE_TABLES = {
        'users': User,
        'games': Game,
        'streams': Stream,
        'videos': Video,
        'clips': Clip,
    }
    AGG_FUNCS = {
        'count': Count, 'sum': Sum, 'avg': Avg, 'max': Max, 'min': Min
    }

    def __init__(self):
        self.tables = []
        self.fields = []
        self.filters = []
        self.aggs = []

    def add_table(self, name): self.tables.append(name)
    def add_field(self, table, field, alias=None):
        key = f"{table}__{field}" if "__" not in field else field
        self.fields.append((key, alias or f"{table}_{field}"))
    def add_filter(self, field, op, value, logical="AND"):
        self.filters.append((field, op, value, logical))
    def add_agg(self, field, func, alias=None):
        self.aggs.append((field, func, alias or f"{func}_{field}"))

    def build_query(self):
        if not self.tables:
            raise ValueError("Nenhuma tabela selecionada")
        main = self.tables[0]
        qs = self.AVAILABLE_TABLES[main].objects.all()
        # joins
        if len(self.tables) > 1:
            # Exemplo básico: se streams e games, faz select_related
            if 'streams' in self.tables and 'games' in self.tables:
                qs = qs.select_related('game')
        # filtros
        q_obj = Q()
        for i,(fld,op,val,log) in enumerate(self.filters):
            lookup = {
                'equals': fld,
                'contains': f"{fld}__icontains",
                'gt': f"{fld}__gt",
                'lt': f"{fld}__lt",
                'gte': f"{fld}__gte",
                'lte': f"{fld}__lte"
            }.get(op)
            cond = Q(**{lookup: val})
            q_obj = cond if i==0 else (q_obj & cond if log=="AND" else q_obj | cond)
        qs = qs.filter(q_obj) if self.filters else qs
        # agregações
        if self.aggs:
            ann = {alias: self.AGG_FUNCS[func](field) for field,func,alias in self.aggs}
            qs = qs.values(*[f for f,_ in self.fields]).annotate(**ann)
        elif self.fields:
            qs = qs.values(*[f for f,_ in self.fields])
        return qs

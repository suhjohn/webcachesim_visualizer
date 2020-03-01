class ISIN:
    def __init__(self, key, value):
        self.key = key
        self.value = value


class EQUALS:
    def __init__(self, key, value):
        self.key = key
        self.value = value

class ISNULL:
    def __init__(self, key):
        self.key = key


class NEQUALS:
    def __init__(self, key, value):
        self.key = key
        self.value = value


def filter_by_parameters(df, params):
    if len(params) < 1:
        raise ValueError

    df1 = df
    for param in params:
        if isinstance(param, ISIN):
            df1 = df1[(getattr(df1, param.key).isin(param.value))]
        elif isinstance(param, EQUALS):
            df1 = df1[(getattr(df1, param.key) == param.value)]
        elif isinstance(param, NEQUALS):
            df1 = df1[(getattr(df1, param.key) != param.value)]
        elif isinstance(param, ISNULL):
            df1 = df1[(getattr(df1, param.key).isnull())]
    return df1.copy()

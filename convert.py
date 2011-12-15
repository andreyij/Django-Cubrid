"""
Convert True/False to 1/0. Needed for Cubrid R4.0
"""
class CubridConvert():
    def boolean_field(self, args):
        args = list(args)
        for i, j in enumerate(args):
            if j == bool(j):
                args[i] = int(j)
        args = tuple(args)
        return args



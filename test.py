import sys
from lark import Lark, Transformer

json_grammar = r"""
    ?value: dict
          | list
          | string
          | SIGNED_NUMBER      -> number
          | "true"             -> true
          | "false"            -> false
          | "null"             -> null

    list : "[" [value ("," value)*] "]"

    dict : "{" [pair ("," pair)*] "}"
    pair : string ":" value

    string : ESCAPED_STRING

    %import common.ESCAPED_STRING
    %import common.SIGNED_NUMBER
    %import common.WS
    %ignore WS
    """

juice_grammar = r"""
?expr: 

"""

# example: func1(num1, "text", func2(DATA, 2*2, [1*1,{}], PROJ(num2)) -> val) -> MERGE

class TreeToJson(Transformer):
    def string(self, s):
        (s,) = s
        return s[1:-1]
    def number(self, n):
        (n,) = n
        return float(n)

    list = list
    pair = tuple
    dict = dict

    null = lambda self, _: None
    true = lambda self, _: True
    false = lambda self, _: False

json_parser = Lark(json_grammar, start='value', lexer='standard', transformer=TreeToJson(),parser='lalr')

text = '{"key": ["item0", "item1", 3.14, true]}'
print(json_parser.parse(text))
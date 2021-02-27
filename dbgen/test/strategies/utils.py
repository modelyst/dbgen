"""utilities for hypothesis testing"""
from hypothesis.strategies import (
    one_of,
    booleans,
    integers,
    text,
    none,
)
from string import ascii_lowercase, ascii_letters, punctuation, digits

password_alpha = digits + ascii_letters + punctuation


anystrat = one_of(text(), booleans(), text(), integers(), none())
nonempty = text(min_size=1)
nonempty_limited = text(min_size=1, max_size=3)
letters = text(min_size=1, alphabet=ascii_lowercase)
letters_complex = text(min_size=1, alphabet=password_alpha)

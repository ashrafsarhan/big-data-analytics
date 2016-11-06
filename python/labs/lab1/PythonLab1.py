# -*- coding: utf-8 -*-
"""
Introduction to Python - Computer Lab1
"""
"""
1. Strings
"""
#a
parrot = 'It is dead, that is what is wrong with it.'
print(parrot)

#b
print(len(parrot))

#c
from string import letters
charSize = 0
for char in parrot:
    if char in letters:
       charSize = charSize+1
print(charSize)

#d
ParrotWords = parrot.split()
print(ParrotWords)

#e
sentence = ' '.join(ParrotWords)
print(sentence)
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

"""
2. Loops and list comprehensions
"""
#a
for num in range(5,11):  #to iterate between 5 to 10
 print('The next number in the loop is %d' % (num))
 
#b 
from random import uniform
while(uniform(0,1) < 0.9):
  print('The random number is smaller than 0.9')
  
#c  
names = ['Ludwig','Rosa','Mona','Amadeus']
for n in names:
    print('The name %s is nice' % (n))
    
#d    
names = ['Ludwig','Rosa','Mona','Amadeus']
nLetters = []
for i, n in enumerate(names):
    nLetters.append(len(n))
print(nLetters)

#e
nLetters = [len(n) for n in names]
print(nLetters)

#f
shortLong = ['long' if len(n)>4 else 'short' for n in names]
print(shortLong)

zipall = zip(names, shortLong)
for n in zipall:
    print('The name %s is a %s name' % (n[0], n[1]))
    
"""
3. Dictionaries
"""
#a
Amadeus = {'Sex':'M', 'Algebra':8, 'History':13}

#b
Rosa = {'Sex':'F', 'Algebra':19, 'History':22}
Mona = {'Sex':'F', 'Algebra':6, 'History':27}
Ludwig = {'Sex':'M', 'Algebra':9, 'History':5}

#c
students = {'Amadeus':Amadeus, 'Rosa':Rosa, 'Mona':Mona, 'Ludwig':Ludwig}
print(students['Amadeus']['History'])

#d
Karl = {'Sex':'M', 'Algebra':14, 'History':10}
students.update({'Karl':Karl})

#e
for key, value in students.iteritems():
    print('Student %s scored %s on the Algebra exam and %s on the History exam' 
          % (key, value['Algebra'], value['History']))

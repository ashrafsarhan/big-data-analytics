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
    
"""
4. Vectors and arrays
"""
#a 
list1 = [1,3,4] 
list2 = [5,6,9]
list1*list2 #TypeError: can't multiply sequence by non-int of type 'list'

#b
from scipy import array, matrix
array1 = array(list1)
array2 = array(list2)
array1*array2 #array([ 5, 18, 36])

#c
matrix1 = array([array1,array2])
print(matrix1)
matrix2 = matrix([[1,0,0],[0,2,0],[0,0,3]])
print(matrix2)
matrix1*matrix2

#d
matrix1.dot(matrix2)

"""
5. Functions
"""
import math
def CircleArea(radius): 
    a = radius**2 * math.pi
    return a
print(CircleArea(10))

#b
def CircleArea(radius):
    if radius > 0:
      a = radius**2 * math.pi
      return a
    else:
      print('Fetal error: The radius must be positive')
      return None
print(CircleArea(-10))

#c
def RectangleArea(base,height):
    a = base * height
    return a
print(RectangleArea(10,10))

#d
"""
ashraf@vostro-v130 ~/Desktop/liu/big-data-analytics/python/labs/lab1 $ python
Python 2.7.12 |Anaconda 4.2.0 (64-bit)| (default, Jul  2 2016, 17:42:40) 
[GCC 4.4.7 20120313 (Red Hat 4.4.7-1)] on linux2
Type "help", "copyright", "credits" or "license" for more information.
Anaconda is brought to you by Continuum Analytics.
Please check out: http://continuum.io/thanks and https://anaconda.org
>>> import Geometry
>>> Geometry.CircleArea(10)
314.1592653589793
>>> Geometry.RectangleArea(10,10)
100
>>> 
"""

    
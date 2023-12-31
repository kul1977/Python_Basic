# Arithmetic operators
a=1
b=2
c=3
d=4

print("-----------------------------------------------------------------------------------")
print("Arithmetic operators")
print("-----------------------------------------------------------------------------------")
print("a+1=",a+1)
print("a-1=",a-1)

print("b/a=",int(b/a))
print("b*a=",b*a)
print("b%a=",b%a)

print("b**c=",b**c)
print("d//b=",d//b)
print("-----------------------------------------------------------------------------------")

# Comparison operators
print("-----------------------------------------------------------------------------------")
print("Comparison operators")
print("-----------------------------------------------------------------------------------")
print("a==1",a==1)
print("a==b",a==b)
print("a!=b",a!=b)
print("a>b",a>b)
print("a<b",a<b)
print("-----------------------------------------------------------------------------------")

# Assignment Operators
print("-----------------------------------------------------------------------------------")
print("Comparison operators")
print("-----------------------------------------------------------------------------------")

a=b
print("a=",a)

a+=b #a=a+b
print("a=",a)

a-=b
print("a=",a)

a*=b
print("a=",a)

a%=b
print("a=",a)
print("-----------------------------------------------------------------------------------")

# Bitwise Operators
print("-----------------------------------------------------------------------------------")
print("Bitwise operators")
print("-----------------------------------------------------------------------------------")
#` = Alt + 096) (~ = Alt + 126)
# Skip for Basic Level dont' require
a=1
print("a&a",a&a)

print("a|b",a|b)


print("a>>1",a>>1) # 0001 >> 0000

print("a<<1",a<<1) # 0001 << 0010
print("a<<2",a<<2) # 0001 << 0100
print("a<<3",a<<3) # 0001 << 1000

print("~1",~1) # 0000000000000001 ~ 1111111111111110
print("~2",~2) # 0000000000000010 ~ 1111111111111101
print("-----------------------------------------------------------------------------------")

print("-----------------------------------------------------------------------------------")
print("Logical & Membership operators")
print("-----------------------------------------------------------------------------------")

arr = (1,2,3,800)
print("(a == 1) and (b == 2)",(a == 1) and (b == 2))
print("(a == 1) or (b == 2)",(a == 1) or (b == 2))

print("(a == 1) and (b == 3)",(a == 1) and (b == 3))
print("(a == 1) or (b == 4)",(a == 1) or (b == 4))
print("not (a and b)",not (a and b))

print("(a in arr)",(a in arr))
print("(799 in arr)",(799 in arr))

print("(a not in arr)",(a not in arr))
print("(799 not in arr)",(799 not in arr))
print("-----------------------------------------------------------------------------------")


print("-----------------------------------------------------------------------------------")
print("Identity operators")
print("-----------------------------------------------------------------------------------")

list1 = []
list2 = []
list3 = list1
 
# case 1
if (list1 == list2):
    print("True")
else:
    print("False")
 
# case 2
if (list1 is list2):
    print("True")
else:
    print("False")
 
# case 3
if (list1 is list3):
    print("True")
else:   
    print("False")
     
# case 4
list3 = list3 + list2
 
if (list1 is list3):
    print("True")
else:   
    print("False")
print("-----------------------------------------------------------------------------------")

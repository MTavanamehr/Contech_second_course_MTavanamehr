a = input("please input an integer number: ")
try:
    int(a)
except:
    print("Unpeacefully converted to integer")
else:
    print(f'You input an integer number: ',a)
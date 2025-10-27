from setuptools import find_packages,setup
from typing import List

def get_requirements()->List[str]:
    '''
    This Function will return list of requirements
    '''
    requirement_lst:List[str] = []
    try:
        with open('/requirements.txt','r') as file:
            lines = file.readlines()
            for line in lines:
                req = line.strip()
                ## ignore empty lines and -e.
                
                if req and not req.startswith('#') and req != '-e .':
                    requirement_lst.append(req)
                    
    except FileNotFoundError:
        print("requirements.txt file not found")
        
    return requirement_lst

setup(
    name='NetworkSecurity',
    version='0.0.1',
    author = 'Charan Sandaka',
    author_email='charan.sandaka5@gmail.com',
    packages=find_packages(),
    install_requires=get_requirements()
)
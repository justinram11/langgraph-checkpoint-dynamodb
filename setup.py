from setuptools import setup, find_packages

setup(
    name='langgraph_checkpoint_dynamodb',
    version='0.1.0',
    author='Justin Ramsey',
    author_email='justin@rflow.io',
    description='DynamoDB Saver for LangGraph Checkpoints',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/justinram11/langgraph-checkpoint-dynamodb',
    packages=find_packages(),
    install_requires=[
        'boto3>=1.17.0',
        'langchain>=0.3.9',
        'langgraph>=0.2.53',
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)
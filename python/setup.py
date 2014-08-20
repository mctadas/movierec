from distutils.core import setup
import py2exe

setup(console=[ 'recommend.py', 'similarity.py', 'similarity_item.py'],
      options = {"py2exe":{"includes":"decimal"}}
      )

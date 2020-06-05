# ESGI_spark

## Prérequis

Sur le poste doivent-être installés :

- Python 3.6        
- Java 1.6
- Pyspark 2.4.5

## Compilation

Depuis la racine du projet, exécuter la commande

``` python setup.py bdist_egg ```

## Execution

Depuis la racine du projet, exécuter la commande

``` spark-submit --deploy-mode client src/Application.py file.csv ```

où file.csv est le fichier de données à traiter
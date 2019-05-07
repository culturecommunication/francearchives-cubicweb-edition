

Schéma SQL pour l'instance de consultation et réplication
=========================================================

L'application créé, au démarrage, un schéma SQL dédié à la
réplication, à l'aide d'un environnement Slony_, des entités "CMS"
dont l'état de workflow est publié vers la base utilisée par
l'instance de consultation des archives.

Ainsi, seules les entités "CMS" qui sont dans l'état "publié"
(`NewsContent`, `BaseContent`, etc.) sont disponibles dans
l'application de consultation.


Création d'un nouvelle instance
-------------------------------

Le script ``postcreate``, lancé à la fin de la création d'une nouvelle
instance de `frarchives_edition`, s'occupe de mettre en place le
schéma SQL (appelé ``published`` par défaut) et de créer les
`triggers` SQL qui gèrent sa maintenance.


Mise en place de la réplication
-------------------------------

La configuration de la réplication Slony_ peut être générée à l'aide
de la commande ``cubicweb-ctl gen-slony <appid> -o <appid>.slonik``.

Cela génère 2 fichiers de commands ``slonik`` (l'outils de gestion de
Slony_).

Le premier génère la configuration du cluster Slony_ (cette
configuration est directement stockée dans les bases de données
intervenanat dans le cluster).

Il faut ensuite démarrer les démons ``slon``, un pour chaque base de
données du cluster (ie. un pour la base maître et un pour chaque base
esclave).

Ensuite, pour activer la réplication, il faut eécuter le second
fichier de commande ``slonik`` (``<appid>_start.slonik``) lance la
réplication.

.. Warning:: Slony_ ne créé pas la ou les bases sur les nœudes
             répliqués, et il ne créé pas non plus les tables. Il faut
             donc avoir créé celles-ci **avant** de lancer les
             commandes ``slonik``.


Migration
---------

Lorsqu'une migration de la base maître (l'instance d'édition) est
nécessaire [...]  **TODO**

.. _Slony: http://slony.info/

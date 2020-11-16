.. -*- mode: rst -*-

=========
 Summary
=========

Edition components for FranceArchives

Ce cube utilise le framework CubicWeb https://www.cubicweb.org/

Les dépendances sont décrites dans le packaging python (`__pkginfo__.py`) et
javascript (`package.json`).

Licence
-------

cubicweb-francearchives is released under the terms of the CeCiLL-C license.

La licence complète peut être trouvée dans ce dépot `LICENCE.txt` et
https://cecill.info/licences/Licence_CeCILL-C_V1-fr.html

Copie d'écran
-------------

.. image:: frarchives_edition.jpg

Documentation supplémentaire
----------------------------

Des éléments supplémentaires de documentation sont dans `doc/` dont notamment

* `doc_dev.rst` explique comment installer le cube avec l'aide de ``pip`` et ``npm``

Tests
-----

Pour lancer les tests, depuis la racine du projet ::

  tox

Les données utilisées pour les tests ne correspondent pas aux données
réelles.

Ces fichiers ne doivent pas être utilisés dans un autre but que celui
de tester la présente application. Le ministère de la Culture décline
toute responsabilité sur les problèmes et inconvénients, de quelque
nature qu'ils soient, qui pourraient survenir en raison d'une
utilisation de ces fichiers à d'autres fins que de tester la présente
application.

Black
-----

Pour lancer **black** ::

  black --config pyproject.toml .


Pour lancer le linter javascript ::

  npm run lint

Pour lancer le linter javascript avec une correction automatique des erreurs triviales ::

  npm run lint -- --fix


Ajouter **black** dans les hooks **hg** ::

créer le script `path_hook` (exemple de code) ::

  #!/bin/sh
  for fpath in $(hg status --no-status --modified --added | grep ".py$") ; do
    black ${fpath}
  done


et appeler le ̀.hg\hgrc` du projet ::

  [hooks]
  precommit = path_to_hook
  pre-amend = path_to_hook

il est possible d'intégrer la config utilisée pour le projet ::

  #!/bin/sh
  for fpath in $(hg status --no-status --modified --added | grep ".py$") ; do
    black --config $1 ${fpath}
  done


et appeler le ̀.hg\hgrc` du projet ::

  [hooks]
  precommit = path_to_hook pyproject.toml
  pre-amend = path_to_hook pyproject.toml


Contributrices et contributeurs
-------------------------------

Voici une liste non exhaustive des personnes ayant contribué à
ce logiciel (ordre alphabetique) :

* Adrien Di Mascio
* Arthur Lutz
* Carine Dengler
* David Douard
* Katia Saurfelt
* Samuel Trégouët

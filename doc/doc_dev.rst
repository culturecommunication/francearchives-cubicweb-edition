
Mise en place de l'environnement (virtualenv)
---------------------------------------------

On suppose qu'on travaille dans un répertoire principal <monprojet>.

On commence par créer un répertoire ``cubes`` vers lequel on fera pointer la
variable d'environnement ``CW_CUBES_PATH`` :

::

    [monprojet]$ mkdir cubes
    [monprojet]$ export CW_CUBES_PATH=$PWD/cubes

On installe le ou les cubes sur lesquels on va développer (c'est-à-dire, *pas
ceux qui sont juste des dépendances*, comme le cube file dans notre cas).

::

    [monprojet]$ cd cubes
    [monprojet]$ hg clone review://clients/SIAF/cubes/francearchives cubes/francearchives
    [monprojet]$ hg clone review://clients/SIAF/cubes/frarchives_edition/ cubes/frarchives_edition

Création d'un virtualenv.

::

    [monprojet]$ virtualenv --system-site-packages venv
    [monprojet]$ . venv/bin/activate

Installation des dépendances :

::

    (venv)[monprojet]$ cd cubes/francearchives
    (venv)[francearchives]$ pip install -e .
    (venv)[monprojet]$ cd ../frarchives_edition
    (venv)[frarchives_edition]$ pip install -e .


.. attention:: L'initialisation de l'instance peut générer une erreur au sujet
    du package ``logilab.common`` non trouvé notamment s'il existe une
    installation système ou utilisateur. C'est une anomalie des paquets Python
    ``logilab-*`` dont le *namespace* ne permet pas une utilisation distribuée
    (par exemple ``logilab-common`` installé dans
    ``/usr/lib/python2.7/dist-packages`` et ``logilab-database`` installé dans
    un virtualenv). Pour contourner ce problème, on peut forcer l'installation
    de ``logilab-common`` dans le virtualenv avec la commande suivante :

    ::

        (venv)$ pip install -I logilab-common


Atelier FranceArchives
======================

Initialisation d'une instance
-----------------------------

Créer une instance du cube frarchives_edition *avec un utilisateur
anonyme*. Donc, activer le virtualenv, puis :

::

    (venv)$ cubicweb-ctl create frarchives_edition atelier

Pour finir, installer un fichier ``pyramid.ini`` dans le répertoire de
l'instance (par ex. ``/etc/cubicweb.d/<appid>/`` contenant) :

::

    [main]
    pyramid.includes =
      cubes.frarchives_edition.atelier
      cubes.frarchives_edition.atelier.auth
    cubicweb.bwcompat = False
    cubicweb.instance = frarchives_edition
    cubicweb.session.secret = <secret>
    cubicweb.auth.authtkt.session.secret = <secret>
    cubicweb.auth.authtkt.persistent.secret = <secret>

Démarrer l'instance
-------------------

::

    $ cubicweb-ctl pyramid -D <appid>


Démarrer un worker
------------------

Pour exécuter les tâches asynchrones on utilise un worker RQ

::

    $ cubicweb-ctl rq-worker <appid>


Développement de l'interface JavaScript
---------------------------------------

S'assurer de disposer de versions de node et npm assez à jour. À l'heure
d'écrire ces lignes, node 4.6 et npm 4.0.5 fonctionnent.

Initialiser et transpiler l'application JavaScript :

::

    $ npm install
    $ npm run watch

Puis lancer l'instance CubicWeb (avec Pyramid) et ouvrir votre navigateur.

/*
 * Copyright © LOGILAB S.A. (Paris, FRANCE) 2016-2019
 * Contact http://www.logilab.fr -- mailto:contact@logilab.fr
 *
 * This software is governed by the CeCILL-C license under French law and
 * abiding by the rules of distribution of free software. You can use,
 * modify and/ or redistribute the software under the terms of the CeCILL-C
 * license as circulated by CEA, CNRS and INRIA at the following URL
 * "http://www.cecill.info".
 *
 * As a counterpart to the access to the source code and rights to copy,
 * modify and redistribute granted by the license, users are provided only
 * with a limited warranty and the software's author, the holder of the
 * economic rights, and the successive licensors have only limited liability.
 *
 * In this respect, the user's attention is drawn to the risks associated
 * with loading, using, modifying and/or developing or reproducing the
 * software by the user in light of its specific status of free software,
 * that may mean that it is complicated to manipulate, and that also
 * therefore means that it is reserved for developers and experienced
 * professionals having in-depth computer knowledge. Users are therefore
 * encouraged to load and test the software's suitability as regards their
 * requirements in conditions enabling the security of their systemsand/or
 * data to be ensured and, more generally, to use and operate it in the
 * same conditions as regards security.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL-C license and that you accept its terms.
 */
/* global CONSULTATION_BASE_URL */
const forEach = require('lodash').forEach;

const {createElement: ce} = require('react'),
      {Link} = require('react-router'),
      {connect} = require('react-redux');

const {default: {buildUrl}} = require('../api');

const {
    showPanel,
} = require('../actions');

const {icon} = require('../components/fa');

const ICON_REGISTRY = {
    section_image: 'picture-o',
    news_image: 'picture-o',
    metadata: 'desktop',
};


function action({url, label, iconName, onClick}) {
    return ce(Link, {to: url, onClick},
              ce('span', {className: 'action'},
                 ce(icon, {name: iconName}),
                 ` ${label}`));
}

function AlertLink({forceShowPanel}) {
    return ce('h3', null,
              action({url: 'alert',
                      label: "Message d'alerte",
                      iconName: 'exclamation-circle',
                      onClick: forceShowPanel}));
}

exports.AlertLink = connect(
    null, {forceShowPanel: showPanel}
)(AlertLink);

function ConsultationLink({uuid, etype, restpath}) {
    // eslint-disable-next-line eqeqeq
    if (window.CONSULTATION_BASE_URL == null) {
        return ce('i', null,
                  'impossible de construire le lien ',
                  ce('br'),
                  'vers la consultation');
    }
    let baseUrl = CONSULTATION_BASE_URL, url;
    if (baseUrl.charAt(baseUrl.length - 1) === '/') {
        baseUrl = baseUrl.substring(0, baseUrl.length - 1)
    }
    if (uuid === null) {
        url = `${baseUrl}/${restpath}`;
    } else {
        url = `${baseUrl}/uuid/${etype}/${uuid}`;
    }
    return ce('h3', null,
              ce('a', {href: url},
                 ce('span', {className: 'action'},
                    ce(icon, {name: 'external-link-square'},
                       ' Vers la consultation'))));
}

exports.ConsultationLink = connect(
    function mapStateToProps(state) {
        const entity = state.getIn(['model', 'entity']),
              uuid = entity.get('uuid'),
              restpath = entity.get('rest_path'),
              etype = entity.get('cw_etype');
        return {etype, uuid, restpath};
    }
)(ConsultationLink);





function AddServiceLink({forceShowPanel}) {
    return ce('h3', null,
              action({url: 'add-service', label: 'Ajouter un service',
                      iconName: 'phone',
                      onClick: forceShowPanel}));
}


exports.AddServiceLink = connect(
    null, {forceShowPanel: showPanel}
)(AddServiceLink);

function AddLink({eid, forceShowPanel}) {
    // eslint-disable-next-line eqeqeq
    if (eid == null) {
        return ce('i', null,
                  "impossible d'ajouter du ",
                  ce('br'),
                  "contenu depuis cette page");
    }
    return ce('h3', null,
              action({url: 'add', label: 'Ajouter',
                      iconName: 'plus',
                      onClick: forceShowPanel}));
}


exports.AddLink = connect(
    function mapStateToProps(state) {
        const eid = state.getIn(['model', 'entity', 'eid']);
        return {eid};
    }, {forceShowPanel: showPanel}
)(AddLink);


function DeleteLink({forceShowPanel}) {
    return ce('h3', null,
              action({url: 'delete', label: 'Supprimer',
                      iconName: 'trash',
                      onClick: forceShowPanel}));
}


exports.DeleteLink = connect(
    null, {forceShowPanel: showPanel}
)(DeleteLink);


function TreeLink({forceShowPanel}) {
    return ce('h3', null,
              action({url: 'tree', label: 'Déplacer ce contenu',
                      iconName: 'sitemap',
                      onClick: forceShowPanel}));
}


exports.TreeLink = connect(
    null, {forceShowPanel: showPanel}
)(TreeLink);


function EditFormLink({forceShowPanel}) {
    return ce('h3', null,
              action({url: 'edit', label: 'Modifier',
                      iconName: 'pencil',
                      onClick: forceShowPanel}));
}


exports.EditFormLink = connect(
    null, {forceShowPanel: showPanel}
)(EditFormLink);

function EditIndexLink({forceShowPanel}) {
    return ce('h3', null,
              action({url: 'edit-index', label: 'Gérer les index',
                      iconName: 'tag',
                      onClick: forceShowPanel}));
}


exports.EditIndexLink = connect(
    null, {forceShowPanel: showPanel}
)(EditIndexLink);


function EditServiceLink({forceShowPanel}) {
    return ce('h3', null,
              action({url: 'edit-service', label: 'Modifier',
                      iconName: 'pencil',
                      onClick: forceShowPanel}));
}

exports.EditServiceLink = connect(
    null, {forceShowPanel: showPanel}
)(EditServiceLink);


function EditServiceListLink({forceShowPanel}) {
    return ce('h3', null,
              action({url: 'edit-service-list', label: 'Modifier',
                      iconName: 'pencil',
                      onClick: forceShowPanel}));
}

exports.EditServiceListLink = connect(
    null, {forceShowPanel: showPanel}
)(EditServiceListLink);



function displayRelated({related, forceShowPanel}){
    const elements = [];
    forEach(related,
            (l, i) => {
                let linkChildren;
                if (ICON_REGISTRY.hasOwnProperty(l.rtype)) {
                    linkChildren = [
                        ce('span', {className: 'action'},
                           icon({name: ICON_REGISTRY[l.rtype]}),
                           ` ${l.title}`),
                    ];
                } else {
                    linkChildren = [
                        ce('span', {className: 'action'}, `${l.title}`),
                    ];
                }
                const options = {pathname: '/editrelated',
                                 query: {name: l.rtype}};
                if (l.rtype === 'cssimage')
                    options.pathname = '/cssimage';
                if (l.rtype.indexOf('index_') !== -1)
                    options.pathname = '/editrelatedindex';
                elements.push(
                    ce('hr', {key: `hr-${i}`}),
                    ce('h3', {key: `h3-${i}`},
                       ce(Link, {to: options,
                                 onClick: forceShowPanel},
                          ...linkChildren)));
            });
    // FIXME "to" value must be unqiue
    return ce('div', null, elements);
}

exports.displayRelated = connect(
    function mapStateToProps(state) {
        return {related: state.getIn(['model', 'related']).toJS()};
    },
    {forceShowPanel: showPanel}
)(displayRelated);



exports.TodoLink = function TodoLink() {
    return ce('h3', null,
              ce('a', {href: buildUrl('non-repris')},
                 ce('span', {className: 'action'},
                    ce(icon, {name: 'sort'}, ' À trier'))));
};


function SearchCWUsersLink({forceShowPanel}) {
    return ce('h3', null,
              action({url: 'cwusers', label: 'Rechercher des utilisateurs',
                      onClick: forceShowPanel}));
}


exports.SearchCWUsersLink = connect(
    null, {forceShowPanel: showPanel}
)(SearchCWUsersLink);


function AddCWUserLink({forceShowPanel}) {
    return ce('h3', null,
              action({url: 'add-user', label: 'Ajouter un utilisateur',
                      iconName: 'user',
                      onClick: forceShowPanel}));
}


exports.AddCWUserLink = connect(
    null, {forceShowPanel: showPanel}
)(AddCWUserLink);


function SearchFaTasksLink({forceShowPanel}) {
    return ce('h3', null,
              action({url: 'fa-tasks', label: 'Rechercher des tâches',
                      onClick: forceShowPanel}));
}


exports.SearchFaTasksLink = connect(
    null, {forceShowPanel: showPanel}
)(SearchFaTasksLink);


function EditHomePageMetataLink({forceShowPanel}) {
    return ce('h3', null,
              action({url: 'homepage-metadata',
                      label: 'Éditer les metadonnées',
                      iconName: 'desktop',
                      onClick: forceShowPanel}));
}

exports.EditHomePageMetataLink = connect(
    null, {forceShowPanel: showPanel}
)(EditHomePageMetataLink);


function PublishTaskLink({forceShowPanel}) {
    return ce('h3', null,
              action({url: 'publish-task',
                      label: 'Publier les IR',
                      iconName: 'send',
                      onClick: forceShowPanel}));
}

exports.PublishTaskLink = connect(
    null, {forceShowPanel: showPanel}
)(PublishTaskLink);


function SameAsLink({forceShowPanel}) {
    return ce('h3', null,
              action({url: 'sameas',
                      label: 'Éditer les liens `same as`',
                      iconName: 'link',
                      onClick: forceShowPanel}));
}

exports.SameAsLink = connect(
    null, {forceShowPanel: showPanel}
)(SameAsLink);


function GroupAuthLink({forceShowPanel}) {
    return ce('h3', null,
              action({url: 'group-auth',
                      label: 'Grouper des autorités',
                      iconName: 'object-group',
                      onClick: forceShowPanel}));
}

exports.GroupAuthLink = connect(
    null, {forceShowPanel: showPanel}
)(GroupAuthLink);


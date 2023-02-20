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

const {Route, Switch} = require('react-router-dom'),
    {createElement: ce} = require('react')

const Editor = require('./containers/editor'),
    {HomePageMetadata} = require('./containers/homepage-metadata'),
    {AlertEditor} = require('./containers/alert'),
    TreeEditor = require('./containers/treeeditor'),
    {ServiceListEditor, AddService} = require('./containers/service'),
    AddContent = require('./containers/add'),
    DeleteContent = require('./containers/delete'),
    {
        IndexEntityRelatedEditor,
        EntityRelatedEditor,
        CssImageEntityRelatedEditor,
    } = require('./containers/relatededitor'),
    {EntityTranslationEditor} = require('./containers/entitytranslationeditor'),
    {SearchCWUsers, AddCWUserForm} = require('./containers/cwusers'),
    {AddGlossaryTermForm} = require('./containers/glossary'),
    {AddFaqForm} = require('./containers/faq'),
    {AddSiteLinkForm} = require('./containers/sitelink'),
    {PublishTask, AddFATask} = require('./containers/fatask'),
    {EditAuthority} = require('./containers/editindex'),
    {EditSameAs} = require('./containers/editsameas'),
    {GroupAuthority} = require('./containers/groupauthority'),
    {SearchFaTasks} = require('./containers/alltasks'),
    {FAMonitoringBord} = require('./containers/fabord'),
    {BlacklistedAuthorities} = require('./containers/blacklisted'),
    {SectionThemes} = require('./containers/sectionthemes')

function renderAutoritiesRelatedEditor(props, targetType) {
    return ce(IndexEntityRelatedEditor, {
        dispatch: props.dispatch,
        location: props.location,
        targetType: targetType,
    })
}

function renderTranslationEditor(props) {
    const language = props.match.params.language
    return ce(EntityTranslationEditor, {
        ...props,
        key: language,
    })
}

const routes = ce(
    Switch,
    {},
    ce(Route, {path: '/edit', component: Editor}),
    ce(Route, {path: '/tree', component: TreeEditor}),
    ce(Route, {path: '/add', component: AddContent}),
    ce(Route, {path: '/alert', component: AlertEditor}),
    ce(Route, {path: '/add-service', component: AddService}),
    ce(Route, {path: '/edit-service-list', component: ServiceListEditor}),
    ce(Route, {path: '/editrelated', component: EntityRelatedEditor}),
    ce(Route, {
        path: '/editlocationauthority',
        render: (props) =>
            renderAutoritiesRelatedEditor(props, 'LocationAuthority'),
    }),
    ce(Route, {
        path: '/editagentauthority',
        render: (props) =>
            renderAutoritiesRelatedEditor(props, 'AgentAuthority'),
    }),
    ce(Route, {
        path: '/editsubjectauthority',
        render: (props) =>
            renderAutoritiesRelatedEditor(props, 'SubjectAuthority'),
    }),
    ce(Route, {path: '/translate_:language', render: renderTranslationEditor}),
    ce(Route, {path: '/delete', component: DeleteContent}),
    ce(Route, {path: '/fatask', component: AddFATask}),
    ce(Route, {path: '/add-user', component: AddCWUserForm}),
    ce(Route, {path: '/cwusers', component: SearchCWUsers}),
    ce(Route, {path: '/add-glossaryterm', component: AddGlossaryTermForm}),
    ce(Route, {path: '/add-faq', component: AddFaqForm}),
    ce(Route, {path: '/add-sitelink', component: AddSiteLinkForm}),
    ce(Route, {path: '/homepage-metadata', component: HomePageMetadata}),
    ce(Route, {path: '/publish-task', component: PublishTask}),
    ce(Route, {path: '/fa-tasks', component: SearchFaTasks}),
    ce(Route, {path: '/fa-bord', component: FAMonitoringBord}),
    ce(Route, {path: '/edit-index', component: EditAuthority}),
    ce(Route, {path: '/sameas', component: EditSameAs}),
    ce(Route, {path: '/group-auth', component: GroupAuthority}),
    ce(Route, {path: '/black-auth', component: BlacklistedAuthorities}),
    ce(Route, {path: '/cssimage', component: CssImageEntityRelatedEditor}),
    ce(Route, {path: '/section-themes', component: SectionThemes}),
)

module.exports = routes

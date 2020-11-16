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

const React = require('react'),
    {Component, createElement: ce} = require('react'),
    PropTypes = require('prop-types'),
    {default: Async} = require('react-select/async'),
    _ = require('lodash'),
    {Link} = require('react-router-dom')
const {parse} = require('query-string')

const {CmsForm} = require('./editor'),
    {spinner} = require('./fa')

const {CustomFieldTemplateConnected} = require('../containers/form')

const {
    default: {
        getRelated,
        getRelatedSchema,
        getRelatedUiSchema,
        relateEntity,
        getAvailableTargets,
        addRelation,
        updateEntity,
    },
} = require('../api')
const {buildFormData} = require('../utils')

const Header = ({entityTitle, title, onAddClick}) => (
    <div>
        <h1>
            "{entityTitle}": {title}
        </h1>
        <div className="cms_add_link">
            <button className="btn btn-default" onClick={onAddClick}>
                Cliquer ici
            </button>{' '}
            pour ajouter une nouvelle entité sous les formulaires existants
        </div>
    </div>
)
Header.propTypes = {
    entityTitle: PropTypes.string.isRequired,
    title: PropTypes.string.isRequired,
    onAddClick: PropTypes.func.isRequired,
}
exports.Header = Header

const DeleteButton = ({entity}) => (
    <Link
        to={{
            pathname: '/delete',
            search: `?eid=${entity.eid}&cw_etype=${entity.cw_etype}`,
        }}
        className="btn btn-default pull-right"
    >
        supprimer
    </Link>
)
DeleteButton.propTypes = {
    entity: PropTypes.shape({
        eid: PropTypes.string.isRequired,
        cw_etype: PropTypes.string.isRequired,
    }).isRequired,
}
exports.DeleteButton = DeleteButton

class EntityRelatedEditor extends Component {
    constructor(props) {
        super(props)
        this.onSubmit = this.onSubmit.bind(this)
        this.sendTargets = this.sendTargets.bind(this)
        this.formRedirects =
            this.props.formRedirects || this.initFormRedirects()
        this.rtypes = props.rtypes.toJS()
        this.state = this.initState(props)
        this.displayFormHeader = this.displayFormHeader.bind(this)
        this.onChangeTargets = this.onChangeTargets.bind(this)
    }

    initFormRedirects() {
        const location_reload = document.location.reload.bind(document.location)
        return {onCancel: location_reload, onSubmit: location_reload}
    }

    initState(props) {
        const query = parse(props.location.search),
            rtype = query.name,
            context = this.rtypes[rtype],
            targetType = props.targetType || null,
            sortTerm = props.sortTerm || null,
            nextEntity = props.entity.toJS(),
            nextState = {rtype, targetType}
        if (this.state !== undefined) {
            // this.state === undefined means we come from constructor
            if (
                this.state.rtype === rtype &&
                nextEntity.cw_etype === this.entity.cw_etype &&
                nextEntity.eid === this.entity.eid
            ) {
                // if rtype and entity have not changed do not fetch related
                // entities and schema
                return nextState
            }
        }
        // unset ``displayCreationForm`` if rtype has changed
        nextState.displayCreationForm = false
        nextState.loading = true
        nextState.title = context.title
        nextState.multiple = context.multiple
        nextState.fetchPossibleTargets = context.fetchPossibleTargets
        this.entity = nextEntity
        Promise.all([
            getRelatedSchema(
                this.entity.cw_etype,
                rtype,
                'creation',
                targetType,
            ),
            getRelatedUiSchema(this.entity.cw_etype, rtype, targetType),
            getRelated(this.entity.cw_etype, this.entity.eid, rtype, {
                sort: sortTerm,
                targetType: targetType,
            }),
        ]).then(([schema, uiSchema, related]) => {
            const targets = related.map(r => ({
                value: r.eid,
                label: r.dc_title,
            }))
            this.setState({
                schema,
                uiSchema,
                related,
                loading: false,
                displayCreationForm: related.length === 0,
                targets: nextState.multiple ? targets : targets[0],
            })
        })
        return nextState
    }

    UNSAFE_componentWillReceiveProps(nextProps) {
        this.setState(this.initState(nextProps))
    }

    onSubmit(entity, create, {formData}) {
        let res
        if (create) {
            res = this.createEntity(entity, formData, this.state.rtype)
        } else {
            res = this.editEntity(entity, formData)
        }
        return res.then(this.formRedirects.onSubmit)
    }

    createEntity(entity, formData, rtype) {
        const {eid, cw_etype} = entity,
            targetType = this.props.targetType
        return relateEntity(cw_etype, eid, rtype, formData, targetType)
    }

    editEntity(entity, formData) {
        return updateEntity(entity.cw_etype, entity.eid, formData)
    }

    entityFormOtherButtons(entity) {
        return [<DeleteButton key="delete" entity={entity} />]
    }

    renderEntityForm(e) {
        const {schema, uiSchema} = this.state,
            formData = buildFormData(e, schema)
        return ce(
            'div',
            {className: 'panel panel-default', key: e.eid},
            ce(
                'div',
                {className: 'panel panel-heading'},
                ce('div', {className: 'panel-title'}, e.dc_title),
            ),
            ce(
                'div',
                {className: 'panel-body'},
                schema
                    ? ce(
                          CmsForm,
                          {
                              schema,
                              uiSchema,
                              onCancel: this.formRedirects.onCancel,
                              formData: formData,
                              formContext: {cw_etype: e.cw_etype, eid: e.eid},
                              FieldTemplate: CustomFieldTemplateConnected,
                              onSubmit: this.onSubmit.bind(this, e, false),
                          },
                          ce(
                              'div',
                              {className: 'btn-group'},
                              ce(
                                  'button',
                                  {
                                      type: 'button',
                                      onClick: () => document.location.reload(),
                                      className: 'btn btn-default',
                                  },
                                  'annuler',
                              ),
                              ce(
                                  'button',
                                  {
                                      type: 'submit',
                                      className: 'btn btn-primary',
                                  },
                                  'enregistrer',
                              ),
                          ),
                          ...this.entityFormOtherButtons(e),
                      )
                    : ce(spinner),
            ),
        )
    }

    sendTargets(e) {
        e.preventDefault()
        const {cw_etype, eid} = this.entity,
            {rtype, targets} = this.state
        return addRelation(
            cw_etype,
            eid,
            rtype,
            Array.isArray(targets) ? targets : [targets],
        ).then(this.formRedirects.onSubmit)
    }

    displayCreationForm() {
        const {schema, uiSchema} = this.state
        let body = ce(CmsForm, {
            schema,
            uiSchema,
            onCancel: this.formRedirects.onCancel,
            formData: {},
            FieldTemplate: CustomFieldTemplateConnected,
            onSubmit: this.onSubmit.bind(this, this.entity, true),
        })
        return ce(
            'div',
            {className: 'panel panel-default'},
            ce('div', {className: 'panel-body'}, body),
        )
    }

    onChangeTargets(value) {
        this.setState({targets: value})
    }

    displayTargetsSearch() {
        const {title, rtype, multiple} = this.state,
            {entity} = this
        function loadOptions(input) {
            if (input.length < 3) {
                return []
            }
            return getAvailableTargets(
                entity.cw_etype,
                rtype,
                entity.eid,
                input,
            ).then(d => d.map(e => ({label: e.title, value: e.eid})))
        }
        return ce(
            'div',
            null,
            ce('h1', null, title),
            ce(
                'form',
                {onSubmit: this.sendTargets},
                ce(Async, {
                    isMulti: multiple,
                    name: 'targets',
                    ignoreAccents: false,
                    loadOptions: _.throttle(loadOptions, 300),
                    value: this.state.targets,
                    noOptionsMessage: () => 'aucune autorité trouvée',
                    isClearable: true,
                    onChange: this.onChangeTargets,
                }),
                ce(
                    'div',
                    {className: 'btn-group'},
                    ce(
                        'button',
                        {
                            type: 'button',
                            className: 'btn btn-default',
                            onClick: this.formRedirects.onCancel,
                        },
                        'annuler',
                    ),
                    ce(
                        'button',
                        {className: 'btn btn-primary', type: 'submit'},
                        'envoyer',
                    ),
                ),
            ),
        )
    }

    displayRelatedEntities() {
        const {related, targetType} = this.state
        let relatedList
        if (targetType === null) {
            relatedList = related
        } else {
            relatedList = related.filter(e => e.cw_etype === targetType)
        }
        return ce(
            'div',
            {className: 'related-entities'},
            relatedList
                ? relatedList.map(this.renderEntityForm, this)
                : ce(spinner),
        )
    }

    displayFormHeader() {
        const {title} = this.state,
            entityTitle = this.entity.dc_title
        return (
            <Header
                entityTitle={entityTitle}
                title={title}
                onAddClick={() => this.setState({displayCreationForm: true})}
            />
        )
    }

    render() {
        // prevent rendering when data have not been loaded yet.
        const {loading, displayCreationForm} = this.state
        if (typeof this.state.schema === 'undefined' || loading) {
            return ce(spinner)
        }
        const {fetchPossibleTargets} = this.state
        if (fetchPossibleTargets) {
            return this.displayTargetsSearch()
        }
        let creationForm = displayCreationForm
            ? this.displayCreationForm()
            : null
        return ce(
            'div',
            null,
            this.displayFormHeader(),
            this.displayRelatedEntities(),
            creationForm,
        )
    }
}

EntityRelatedEditor.propTypes = {
    entity: PropTypes.object,
    rtypes: PropTypes.object,
    dispatch: PropTypes.func.isRequired,
    location: PropTypes.object,
    targetType: PropTypes.string,
    formRedirects: PropTypes.shape({
        onCancel: PropTypes.func,
        onSubmit: PropTypes.func,
    }),
}

exports.EntityRelatedEditor = EntityRelatedEditor

class CssImageEntityRelatedEditor extends EntityRelatedEditor {
    entityFormOtherButtons() {
        return []
    }
}

exports.CssImageEntityRelatedEditor = CssImageEntityRelatedEditor

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

const {Component, createElement: ce} = require('react'),
    PropTypes = require('prop-types'),
    {default: Async} = require('react-select/async'),
    _ = require('lodash')
const {parse} = require('query-string')

const {CmsForm} = require('./editor'),
    {spinner, button} = require('./fa')
const {Alert} = require('../components/error')

const {CustomFieldTemplateConnected} = require('../containers/form')

const {
    default: {
        getRelated,
        getRelatedSchema,
        getRelatedUiSchema,
        relateEntity,
        getAvailableTargets,
        addRelation,
    },
} = require('../api')

const BootstrapTable = require('react-bootstrap-table-next').default
const {default: paginationFactory} = require('react-bootstrap-table2-paginator')

function RelatedEntitiesListEditor(props) {
    const {related, targetType, i18ntargetType} = props
    let relatedList
    if (targetType === null) {
        relatedList = related
    } else {
        relatedList = related
            .filter(e => e.cw_etype === targetType)
            .map(e => ({
                eid: e.eid,
                label: e.dc_title,
                link: ce(
                    'a',
                    {
                        href: e.absoluteUrl,
                        target: '_blank',
                    },
                    e.dc_title,
                ),
            }))
    }
    const columns = [
        {
            dataField: 'link',
            text: 'Autorités',
            sort: true,
            sortFunc: (acomp, bcomp, order) => {
                const a = acomp.props.children.toLowerCase(),
                    b = bcomp.props.children.toLowerCase()
                let res
                if (b > a) {
                    res = order === 'desc' ? 1 : -1
                } else {
                    res = order === 'desc' ? -1 : 1
                }
                return res
            },
        },
        {
            dataField: 'action',
            text: '',
            formatter: (cell, row) => {
                return ce(button, {
                    title: 'supprimer la relation',
                    onClick: () => props.deleteIndex(row.eid),
                    name: 'trash',
                })
            },
            style: {width: '5%'},
        },
    ]
    const defaultSorted = [
        {
            dataField: 'link',
            order: 'asc',
        },
    ]
    const relatedTable = ce(
        'div',
        null,
        ce(
            'h2',
            null,
            ce(
                'span',
                {style: {textTransform: 'capitalize'}},
                `${i18ntargetType}`,
            ),
            ` liées (${relatedList.length})`,
        ),
        ce(BootstrapTable, {
            keyField: 'eid',
            data: relatedList,
            columns: columns,
            defaultSorted: defaultSorted,
            pagination: paginationFactory(),
        }),
    )
    return ce(
        'div',
        {className: 'related-entities'},
        relatedList ? relatedTable : ce(spinner),
    )
}

RelatedEntitiesListEditor.propTypes = {
    targetType: PropTypes.string.required,
    i18ntargetType: PropTypes.string.required,
    related: PropTypes.array.isRequired,
    deleteIndex: PropTypes.func.isRequired,
    dispatch: PropTypes.func.isRequired,
}

class AuthoritiesEditor extends Component {
    constructor(props, ctx) {
        super(props, ctx)
        ;(this.entity = props.entity),
            (this.state = {
                loading: true,
                displayCreationForm: false,
                selectedAuthority: null,
                searchMatch: null,
                searchValue: null,
            })
        this.onChangeAuthority = this.onChangeAuthority.bind(this)
        const wrapsStateReinit = method => (...args) => {
            this.resetSearchRelatedStates()
            return method(...args)
        }
        this.createAuthority = wrapsStateReinit(this.props.createAuthority)
        this.createRelationWithAuthority = wrapsStateReinit(
            this.props.createRelationWithAuthority,
        )
    }

    componentDidMount() {
        const {targetType, rtype} = this.props
        Promise.all([
            getRelatedSchema(
                this.entity.cw_etype,
                rtype,
                'creation',
                targetType,
            ),
            getRelatedUiSchema(this.entity.cw_etype, rtype, targetType),
        ]).then(([schema, uiSchema]) => {
            this.setState({schema, uiSchema, loading: false})
        })
    }

    resetSearchRelatedStates() {
        this.setState({
            searchMatch: null,
            searchValue: null,
            selectedAuthority: null,
            displayCreationForm: false,
        })
    }

    onChangeAuthority(value) {
        if (value === null || value.length === 0) {
            this.setState({searchMatch: null})
        }
        this.setState({selectedAuthority: value, searchValue: null})
    }

    displayAuthorityLink(eid) {
        const link = `${window.BASE_URL}${this.props.targetType}/${eid}`
        return ce(
            'a',
            {
                className: 'col-xs-1 fa-stack fa-lg url_link',
                href: link,
                target: '_blank',
            },
            ce('i', {className: 'fa fa-circle fa-stack-2x'}),
            ce('i', {className: 'fa fa-arrow-right fa-stack-1x fa-inverse'}),
        )
    }

    displayCreationForm() {
        const {schema, uiSchema, searchValue, selectedAuthority} = this.state
        const formData =
            searchValue !== null && selectedAuthority === null
                ? {label: searchValue}
                : {}
        let body = ce(CmsForm, {
            schema,
            uiSchema,
            onCancel: event => {
                event.preventDefault()
                this.resetSearchRelatedStates()
                this.setState({displayCreationForm: false})
            },
            formData,
            FieldTemplate: CustomFieldTemplateConnected,
            onSubmit: this.createAuthority.bind(this, this.entity),
        })
        return ce(
            'div',
            {className: 'panel panel-default'},
            ce('div', {className: 'panel-body'}, body),
        )
    }

    render() {
        const {loading} = this.state
        if (typeof this.state.schema === 'undefined' || loading) {
            return ce(spinner)
        }
        const {entity} = this,
            {targetType, rtype} = this.props,
            {
                displayCreationForm,
                searchMatch,
                searchValue,
                selectedAuthority,
            } = this.state
        const resolveAvailableTargets = (d => {
            const searchMatch = d.length !== 0
            this.setState({searchMatch})
            return d.map(e => ({label: e.title, value: e.eid}))
        }).bind(this)

        function loadOptions(input) {
            if (input.length < 3) {
                return []
            }
            this.setState({searchValue: input})
            const params = targetType !== null ? {target_type: targetType} : {}
            return getAvailableTargets(
                entity.cw_etype,
                rtype,
                entity.eid,
                input,
                params,
            ).then(resolveAvailableTargets)
        }
        let select = null
        const creationFormButtonMsg =
            searchMatch === false
                ? `aucun résultat trouvé, cliquez pour ajouter "${searchValue}"`
                : 'ajouter une nouvelle autorité'
        const linkMsg =
            selectedAuthority !== null
                ? "voir l'autorité séléctionnée"
                : 'Aucune autorité séléctionnée'
        return ce(
            'div',
            {},
            ce(
                'div',
                {className: 'authorities-search-form'},
                ce('h2', null, `Ajouter une autorité`),
                ce(
                    'form',
                    {
                        onSubmit: e => {
                            e.preventDefault()
                            this.createRelationWithAuthority(
                                e,
                                selectedAuthority,
                            )
                        },
                    },
                    select,
                    ce(
                        'div',
                        {className: 'form-group container-fluid'},
                        ce(
                            'div',
                            {className: 'row'},
                            ce(Async, {
                                isMulti: false,
                                name: 'targets',
                                placeholder: 'Rechercher une autorité',
                                loadOptions: _.throttle(
                                    loadOptions.bind(this),
                                    300,
                                ),
                                value: selectedAuthority,
                                noOptionsMessage: () => null,
                                isClearable: true,
                                onFocus: () => this.setState({message: null}),
                                onChange: this.onChangeAuthority,
                                className: 'col-xs-10',
                            }),
                            ce(
                                'span',
                                {
                                    className: 'control-label col-xs-1',
                                },
                                linkMsg,
                            ),
                            selectedAuthority !== null
                                ? this.displayAuthorityLink(
                                      selectedAuthority.value,
                                  )
                                : null,
                        ),
                    ),
                    ce(
                        'div',
                        {className: 'btn-group'},
                        ce(
                            'button',
                            {
                                className:
                                    selectedAuthority !== null
                                        ? 'btn btn-default'
                                        : 'btn btn-primary',
                                type: 'button',
                                onClick: event => {
                                    event.preventDefault()
                                    this.setState({
                                        displayCreationForm: true,
                                        message: null,
                                    })
                                },
                            },
                            creationFormButtonMsg,
                        ),
                        selectedAuthority !== null
                            ? ce(
                                  'button',
                                  {
                                      className: 'btn btn-primary',
                                      type: 'submit',
                                  },
                                  "ajouter l'autorité trouvée",
                              )
                            : null,
                    ),
                ),
            ),
            displayCreationForm ? this.displayCreationForm() : null,
        )
    }
}

AuthoritiesEditor.propTypes = {
    dispatch: PropTypes.func.isRequired,
    entity: PropTypes.object.isRequired,
    rtype: PropTypes.string.isRequired,
    targetType: PropTypes.string.required,
    createAuthority: PropTypes.func.isRequired,
    createRelationWithAuthority: PropTypes.func.isRequired,
}

class IndexEntityRelatedEditor extends Component {
    constructor(props) {
        super(props)
        this.updateMessage = newMsg => this.setState({message: newMsg})
        this.rtypes = props.rtypes.toJS()
        this.state = this.initState(props)
        this.displayFormHeader = this.displayFormHeader.bind(this)
        this.loadRelated = this.loadRelated.bind(this)
    }

    UNSAFE_componentWillReceiveProps(nextProps) {
        this.setState(this.initState(nextProps))
    }

    initState(props) {
        const query = parse(props.location.search),
            rtype = query.name,
            context = this.rtypes[rtype],
            multiple = context.multiple,
            targetType = props.targetType || null,
            nextEntity = props.entity.toJS(),
            nextState = {rtype, targetType, multiple}
        if (this.state !== undefined) {
            // this.state === undefined means we come from constructor
            if (
                this.state.rtype === rtype &&
                this.state.targetType === targetType &&
                nextEntity.cw_etype === this.entity.cw_etype &&
                nextEntity.eid === this.entity.eid
            ) {
                // if rtype and entity have not changed do not fetch related
                // entities and schema
                return nextState
            }
        }
        if (context.etargets) {
            nextState.i18ntargetType =
                context.titles[context.etargets.indexOf(targetType)]
        } else {
            nextState.i18ntargetType = context.title
        }
        nextState.multiple = context.multiple
        nextState.fetchPossibleTargets = context.fetchPossibleTargets
        nextState.loading = true
        nextState.message = null
        this.entity = nextEntity
        this.loadRelated(rtype, targetType, multiple)
        return nextState
    }

    loadRelated(rtype, targetType, multiple) {
        Promise.all([
            getRelated(this.entity.cw_etype, this.entity.eid, rtype),
        ]).then(([related]) => {
            const targets = related.map(r => ({
                value: r.eid,
                label: r.dc_title,
            }))
            this.setState({
                related,
                loading: false,
                targets: multiple ? targets : targets[0],
            })
        })
    }

    displayFormHeader() {
        const {i18ntargetType} = this.state,
            entity = this.entity,
            link = `"${entity.dc_title}" : gérer les ${i18ntargetType}`
        return ce('h1', null, link)
    }

    displayCloseFormBtn() {
        return ce(
            'button',
            {
                type: 'button',
                onClick: () => document.location.reload(),
                className: 'btn btn-primary pull-right',
            },
            'fermer',
        )
    }

    deleteIndex(indexEid) {
        const {related, rtype, targetType, multiple} = this.state,
            targets = related.map(r => ({value: r.eid, label: r.dc_title})),
            {cw_etype, eid} = this.entity,
            toDelete = targets
                .map(el => {
                    if (el.value === indexEid) return el.label
                })
                .filter(r => r !== undefined)
        const newRelated = related.filter(el => el.eid !== indexEid),
            newTargets = targets.filter(el => el.value !== indexEid)
        const allrelated = new Set(
            newRelated.map(r => r.eid).concat(newTargets.map(t => t.value)),
        )
        return addRelation(
            cw_etype,
            eid,
            rtype,
            Array.from(allrelated).map(eid => ({value: eid})),
        )
            .then(() => {
                this.updateMessage({
                    type: 'success',
                    text: `Relation avec l'autorité "${toDelete}" supprimée.`,
                })
                this.loadRelated(rtype, targetType, multiple)
            })
            .catch(e => {
                this.updateMessage({
                    type: 'danger',
                    text: `Relation avec l'autorité "${toDelete}" n'a pas pu être supprimée.`,
                })
                console.error(e)
            })
    }

    createRelationWithAuthority(e, selectedAuthority) {
        const {cw_etype, eid} = this.entity,
            {targets, rtype, targetType, multiple} = this.state,
            allauthorities = []
        allauthorities.push(selectedAuthority)
        if (Array.isArray(targets)) {
            allauthorities.push(...targets)
        } else {
            allauthorities.push(targets)
        }
        return addRelation(cw_etype, eid, rtype, allauthorities)
            .then(() => {
                this.updateMessage({
                    type: 'success',
                    text: `La relation avec l'autorité "${selectedAuthority.label}" ajoutée.`,
                })
                this.loadRelated(rtype, targetType, multiple)
            })
            .catch(e => {
                this.updateMessage({
                    type: 'danger',
                    text: `La relation avec l'autorité "${selectedAuthority.label}" n'a pas pu être ajoutée.`,
                })
                console.error(e)
            })
    }

    createAuthority(entity, {formData}) {
        const {targetType, rtype, multiple} = this.state,
            {cw_etype, eid} = this.entity
        return relateEntity(cw_etype, eid, rtype, formData, targetType)
            .then(() => {
                this.updateMessage({
                    type: 'success',
                    text: `L'autorité "${formData.label}" a été créé et liée.`,
                })
                this.loadRelated(rtype, targetType, multiple)
            })
            .catch(e => {
                this.updateMessage({
                    type: 'danger',
                    text: `L'autorité "${formData.label}" n'as pas pu être créé.`,
                })
                console.error(e)
            })
    }

    render() {
        // prevent rendering when data have not been loaded yet.
        if (this.state.loading) {
            return ce(spinner)
        }
        return ce(
            'div',
            null,
            this.displayFormHeader(),
            ce(Alert, {message: this.state.message}),
            ce(AuthoritiesEditor, {
                entity: this.entity,
                rtype: this.state.rtype,
                targetType: this.state.targetType,
                dispatch: this.props.dispatch,
                createAuthority: this.createAuthority.bind(this),
                createRelationWithAuthority: this.createRelationWithAuthority.bind(
                    this,
                ),
            }),
            ce(RelatedEntitiesListEditor, {
                targetType: this.state.targetType,
                i18ntargetType: this.state.i18ntargetType,
                related: this.state.related,
                deleteIndex: this.deleteIndex.bind(this),
                dispatch: this.props.dispatch,
            }),
            this.displayCloseFormBtn(),
        )
    }
}

IndexEntityRelatedEditor.propTypes = {
    entity: PropTypes.object,
    rtypes: PropTypes.object,
    dispatch: PropTypes.func.isRequired,
    location: PropTypes.object,
    targetType: PropTypes.string,
}

exports.IndexEntityRelatedEditor = IndexEntityRelatedEditor

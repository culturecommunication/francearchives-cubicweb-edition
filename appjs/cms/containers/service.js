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
/* globals $ */

const React = require('react'),
    {Component, createElement: ce} = require('react'),
    PropTypes = require('prop-types'),
    {default: Async} = require('react-select/async'),
    {connect} = require('react-redux'),
    _ = require('lodash'),
    Immutable = require('immutable'),
    {
        BootstrapTable: BT,
        TableHeaderColumn: THC,
    } = require('react-bootstrap-table'),
    Button = require('react-bootstrap/cjs/Button'),
    Modal = require('react-bootstrap/cjs/Modal'),
    Form = require('react-bootstrap/cjs/Form')

const {
    default: {
        jsonFetch,
        getSchema,
        createEntity,
        getUiSchema,
        updateEntity,
        getEntity,
        getRelated,
        getRelatedSchema,
        getRelatedUiSchema,
        relateEntity,
        addRelation,
        deleteRelation,
        getAvailableTargets,
    },
} = require('../api')
const {buildFormData} = require('../utils')
const {spinner, icon, button} = require('../components/fa'),
    {showErrors} = require('../actions'),
    {CmsForm} = require('../components/editor'),
    DeleteForm = require('../components/delete'),
    EntityRelatedEditor = require('../components/relatededitor')

const {CustomFieldTemplateConnected} = require('./form')

class SelectAnnex extends Component {
    constructor(props) {
        super(props)
        this.state = {annex: null, loading: true, bodyType: null}
        this.validate = this.validate.bind(this)
        this.deleteAnnex = this.deleteAnnex.bind(this)
    }

    UNSAFE_componentWillReceiveProps(nextProps) {
        if (this.props.service.eid === nextProps.service.eid) {
            return
        }
        this.fetchData(nextProps.service)
    }

    fetchData(service) {
        this.setState({loading: true, annex: null, bodyType: null})
        getRelated('service', service.eid, 'annex_of').then((services) => {
            if (this.unmounted) {
                // do not call setState on an unmounted Component
                return
            }
            if (services.length) {
                this.setState({
                    annex: {
                        label: services[0].dc_title,
                        value: services[0].eid,
                    },
                })
            } else {
                this.setState({title: null})
            }
            this.setState({loading: false})
        })
    }

    componentDidMount() {
        this.fetchData(this.props.service)
    }

    componentWillUnmount() {
        this.unmounted = true
    }

    validate() {
        this.setState({bodyType: null})
        const {service} = this.props,
            {annex} = this.state
        if (annex) {
            return addRelation('service', service.eid, 'annex_of', [annex])
        } else {
            return deleteRelation('service', service.eid, 'annex_of')
        }
    }

    deleteAnnex() {
        this.setState({bodyType: null, annex: null})
        const {service} = this.props
        return deleteRelation('service', service.eid, 'annex_of')
    }

    render() {
        const {loading, annex, bodyType} = this.state,
            {service} = this.props
        let body

        function loadOptions(input) {
            if (input.length < 3) {
                return []
            }
            return getAvailableTargets(
                'service',
                'annex_of',
                service.eid,
                input,
            ).then((d) => d.map((e) => ({label: e.title, value: e.eid})))
        }

        if (loading) {
            body = ce(spinner)
        } else if (bodyType === 'select') {
            body = ce(
                'div',
                null,
                ce(Async, {
                    loadOptions: _.throttle(loadOptions, 300),
                    value: annex,
                    onBlurResetsInput: false,
                    placeholder: 'Rechercher',
                    noOptionsMessage: () => null,
                    onChange: (value) => this.setState({annex: value}),
                }),
                ce(
                    'button',
                    {
                        className: 'btn-default btn',
                        onClick: () => this.setState({bodyType: null}),
                    },
                    'annuler',
                ),
                ce(
                    'button',
                    {className: 'btn-primary btn', onClick: this.validate},
                    'valider',
                ),
            )
        } else if (bodyType === 'delete') {
            body = ce(
                'div',
                null,
                'voulez supprimer ce lien annexe ? ',
                ce(
                    'button',
                    {
                        className: 'btn-default btn',
                        onClick: () => this.setState({bodyType: null}),
                    },
                    'annuler',
                ),
                ce(
                    'button',
                    {className: 'btn-primary btn', onClick: this.deleteAnnex},
                    'valider',
                ),
            )
        } else if (bodyType === null && annex !== null) {
            body = ce(
                'span',
                null,
                annex.label,
                ' ',
                ce(
                    'button',
                    {
                        className: 'btn btn-link',
                        onClick: () => this.setState({bodyType: 'select'}),
                    },
                    'modifier',
                ),
                ce(
                    'button',
                    {
                        className: 'btn btn-link',
                        onClick: () => this.setState({bodyType: 'delete'}),
                    },
                    'supprimer le lien',
                ),
            )
        } else {
            body = ce(
                'div',
                null,
                ce('i', null, 'pas de service relié'),
                ' ',
                ce(
                    'button',
                    {
                        className: 'btn btn-link',
                        onClick: () => this.setState({bodyType: 'select'}),
                    },
                    'modifier',
                ),
            )
        }
        return ce(
            'div',
            null,
            ce(
                'h2',
                null,
                `Le service "${service.dc_title}" est une annexe de :`,
            ),
            body,
        )
    }
}
SelectAnnex.propTypes = {
    service: PropTypes.object.isRequired,
}

class AddService extends Component {
    constructor(props) {
        super(props)
        this.state = {schema: null, uiSchema: null, formData: null}
        this.onSubmit = this.onSubmit.bind(this)
    }

    onSubmit(ev) {
        this.setState({formData: ev.formData})
        return createEntity('service', ev.formData).then((doc) => {
            if (doc.errors && doc.errors.length) {
                this.props.dispatch(showErrors(doc.errors))
                return
            } else if (doc.absoluteUrl) {
                document.location.replace(doc.absoluteUrl)
            }
        })
    }

    componentDidMount() {
        Promise.all([
            getSchema('service', null, 'creation'),
            getUiSchema('service'),
        ]).then(([schema, uiSchema]) => this.setState({schema, uiSchema}))
    }

    render() {
        const {schema, uiSchema, formData} = this.state
        let body
        if (schema === null) {
            body = ce(spinner)
        } else {
            body = ce(CmsForm, {
                schema,
                uiSchema,
                formData,
                FieldTemplate: CustomFieldTemplateConnected,
                onSubmit: this.onSubmit,
            })
        }
        return ce(
            'div',
            null,
            ce('h1', null, 'Ajouter un nouveau service'),
            body,
        )
    }
}

AddService.propTypes = {
    dispatch: PropTypes.func.isRequired,
}

exports.AddService = connect()(AddService)

class ServiceEditor extends Component {
    constructor(props) {
        super(props)
        this.state = {
            formData: null,
            schema: null,
            uiSchema: null,
            loadingEditor: true,
        }
        this.onEditSubmit = this.onEditSubmit.bind(this)
    }

    UNSAFE_componentWillReceiveProps(nextProps) {
        if (nextProps.entity.eid === this.props.entity.eid) {
            return
        }
        this.fetchData(nextProps.entity.eid)
    }

    fetchData(eid) {
        this.setState({loadingEditor: true, eid})
        const promises = [
            getEntity('service', eid),
            getSchema('service', eid, 'edition'),
        ]
        if (this.state.uiSchema) {
            promises.push(Promise.resolve(this.state.uiSchema))
        } else {
            promises.push(getUiSchema('service'))
        }
        Promise.all(promises).then(([entity, schema, uiSchema]) =>
            this.setState({
                uiSchema,
                formData: entity,
                schema,
                loadingEditor: false,
            }),
        )
    }

    componentDidMount() {
        this.fetchData(this.props.entity.eid)
    }

    onEditSubmit(ev) {
        const {eid} = this.props.entity
        this.setState({formData: ev.formData})
        return updateEntity('service', eid, ev.formData).then((doc) => {
            if (doc.errors && doc.errors.length) {
                this.props.dispatch(showErrors(doc.errors))
                return
            }
            document.location.reload()
        })
    }

    render() {
        const {loadingEditor, schema, uiSchema, formData} = this.state
        let form
        if (loadingEditor) {
            form = ce(spinner)
        } else {
            form = ce(CmsForm, {
                schema,
                uiSchema,
                formData: buildFormData(formData, schema),
                FieldTemplate: CustomFieldTemplateConnected,
                onSubmit: this.onEditSubmit,
            })
        }
        return ce(
            'div',
            null,
            ce('h1', null, "Édition de l'annuaire de service"),
            form,
        )
    }
}

ServiceEditor.propTypes = {
    dispatch: PropTypes.func.isRequired,
    entity: PropTypes.object.isRequired,
}

exports.ServiceEditor = connect()(ServiceEditor)

function getJsonUrl() {
    const pathname = document.location.pathname,
        isdpt = pathname.includes('departements'),
        url = isdpt
            ? '/annuaire/departements' + document.location.search
            : '/annuaire/' + /service\/(\d+)/.exec(pathname)[1]
    return url
}

function metadataPrefixIsNomina(url) {
    return url.includes('metadataPrefix=nomina')
}

function OAIPublishButton({entityData}) {
    const [loading, setLoading] = React.useState(false)
    const [showPublishModal, setShowPublishModal] = React.useState(false)
    const [doImport, setDoImport] = React.useState(true)
    const [fromLastImport, setFromLastImport] = React.useState(false)
    const [recordsLimit, setRecordsLimit] = React.useState({
        enabled: false,
        value: '1',
    })
    const [recordsInCSV, setRecordsInCSV] = React.useState(100000)
    const invalidRecordsLimit =
        recordsLimit.enabled &&
        (Number.isNaN(Number.parseInt(recordsLimit.value)) ||
            Number.parseInt(recordsLimit.value) < 1)
    const invalidRecordsInCSV =
        recordsInCSV.value !== undefined &&
        (Number.isNaN(Number.parseInt(recordsInCSV.value)) ||
            Number.parseInt(recordsInCSV.value) < 1)
    function createTask(evt) {
        evt.preventDefault()
        // onSubmit event, updating the OAIRepository is called later
        const {eid, name} = entityData
        let taskName = `import-oai for OAIRepository #${eid}`
        if (name !== undefined) {
            taskName += `(${name})`
        }
        if (invalidRecordsLimit || invalidRecordsInCSV) return
        setLoading(true)
        return createEntity(
            'RqTask',
            {
                name: `import_oai`,
                title: taskName,
                oairepository: eid,
                dry_run: !doImport,
                force_refresh: !fromLastImport,
                records_limit: recordsLimit.enabled
                    ? Number.parseInt(recordsLimit.value)
                    : null,
                csv_rows_limit: recordsInCSV.value,
            },
            'import_oai',
        ).then((doc) => {
            if (doc.errors && doc.errors.length) {
                this.props.dispatch(showErrors(doc.errors))
                return
            } else if (doc.absoluteUrl) {
                document.location.replace(doc.absoluteUrl)
            }
        })
    }

    const Spinner = spinner
    const Icon = icon
    return (
        <React.Fragment>
            <Button
                className="pull-right"
                title="moissonner cet entrepôt"
                onClick={(evt) => {
                    if (metadataPrefixIsNomina(entityData.url)) {
                        setShowPublishModal(true)
                    } else {
                        createTask(evt)
                    }
                }}
            >
                <Icon name="play" />
                moissonner
            </Button>

            <Modal
                show={showPublishModal}
                onHide={() => setShowPublishModal(false)}
                size="lg"
                centered
            >
                <Modal.Header closeButton>
                    <Modal.Title>Paramètres du moissonnage</Modal.Title>
                </Modal.Header>
                <Form onSubmit={createTask} disabled={loading}>
                    <Modal.Body>
                        <Form.Group className="mb-3" controlId="doImport">
                            <Form.Check
                                label="Importer les données moissonnées dans l'application"
                                checked={doImport}
                                className="mb-3"
                                onChange={(evt) =>
                                    setDoImport(evt.currentTarget.checked)
                                }
                            />
                            <Form.Check
                                label="Importer uniquement les nouvelles données et les données modifiées depuis le dernier import"
                                checked={fromLastImport}
                                className="mb-3"
                                onChange={(evt) =>
                                    setFromLastImport(evt.currentTarget.checked)
                                }
                            />
                        </Form.Group>
                        <Form.Group className="mb-3" controlId="recordsLimit">
                            <Form.Check
                                label="Limiter le nombre d'enregistrements à moissonner"
                                value={recordsLimit.enabled}
                                onChange={(evt) =>
                                    setRecordsLimit({
                                        ...recordsLimit,
                                        enabled: evt.currentTarget.checked,
                                    })
                                }
                            />
                            <Form.Control
                                type={recordsLimit.enabled ? 'number' : 'text'}
                                disabled={!recordsLimit.enabled}
                                min={1}
                                value={
                                    recordsLimit.enabled
                                        ? recordsLimit.value
                                        : ''
                                }
                                onChange={(evt) =>
                                    setRecordsLimit({
                                        ...recordsLimit,
                                        value: evt.currentTarget.value,
                                    })
                                }
                            />
                        </Form.Group>
                        <Form.Group className="mb-3" controlId="recordsInCSV">
                            Limiter le nombre d'enregistrements moissonnés par
                            fichier
                            <Form.Control
                                type="number"
                                min={1}
                                value={recordsInCSV.value}
                                defaultValue={100000}
                                onChange={(evt) =>
                                    setRecordsInCSV({
                                        ...recordsInCSV,
                                        value: evt.currentTarget.value,
                                    })
                                }
                            />
                        </Form.Group>
                    </Modal.Body>
                    <Modal.Footer>
                        <Button
                            type="button"
                            variant="secondary"
                            onClick={() => setShowPublishModal(false)}
                        >
                            Annuler
                        </Button>
                        <Button
                            type="submit"
                            variant="primary"
                            onClick={createTask}
                            disabled={
                                loading ||
                                invalidRecordsLimit ||
                                invalidRecordsInCSV
                            }
                        >
                            {loading ? <Spinner /> : <Icon name="play" />}
                            Moissonner
                        </Button>
                    </Modal.Footer>
                </Form>
            </Modal>
        </React.Fragment>
    )
}

OAIPublishButton.propTypes = {
    dispatch: PropTypes.func.isRequired,
    entityData: PropTypes.shape({
        eid: PropTypes.integer,
        name: PropTypes.string,
        url: PropTypes.string,
    }).isRequired,
}

const OAIRepositoryEditForm = ({
    schema,
    uiSchema,
    entity,
    formRedirects,
    dispatch,
}) => {
    const [formData, setFormData] = React.useState(
        buildFormData(entity, schema),
    )
    const entityData = {...formData, eid: entity.eid}

    function handleChange({formData}) {
        setFormData(buildFormData(formData, schema))
    }

    function handleSubmit(formData) {
        return updateEntity(entity.cw_etype, entity.eid, formData).then(
            formRedirects.onSubmit,
        )
    }

    return (
        <div className="panel panel-default">
            <div className="panel panel-heading">
                <div className="panel-title">{entity.dc_title}</div>
            </div>
            <div className="panel-body">
                <CmsForm
                    schema={schema}
                    uiSchema={uiSchema}
                    formData={formData}
                    onCancel={formRedirects.onCancel}
                    formContext={{cw_etype: entity.cw_etype, eid: entity.eid}}
                    FieldTemplate={CustomFieldTemplateConnected}
                    onChange={handleChange}
                    onSubmit={({formData}) => handleSubmit(formData)}
                >
                    <div className="btn-group">
                        <button
                            className="btn btn-default"
                            type="button"
                            onClick={() => document.location.reload()}
                        >
                            annuler
                        </button>
                        <button className="btn btn-primary" type="submit">
                            enregistrer
                        </button>
                    </div>
                    <OAIPublishButton
                        entityData={entityData}
                        dispatch={dispatch}
                    />
                    <EntityRelatedEditor.DeleteButton entity={entity} />
                </CmsForm>
            </div>
        </div>
    )
}
OAIRepositoryEditForm.propTypes = {
    entity: PropTypes.object.isRequired,
    schema: PropTypes.object.isRequired,
    uiSchema: PropTypes.object.isRequired,
    formRedirects: PropTypes.object.isRequired,
    dispatch: PropTypes.func.isRequired,
}

const OAIRelatedEditor = ({dispatch, entity, formRedirects}) => {
    const [displayCreationForm, setDisplayCreationForm] = React.useState(false)
    const [loading, setLoading] = React.useState(true)
    const [schema, setSchema] = React.useState(null)
    const [uiSchema, setUiSchema] = React.useState(null)
    const [related, setRelated] = React.useState(null)
    React.useEffect(() => {
        setLoading(true)
        setDisplayCreationForm(false)
        Promise.all([
            getRelatedSchema(
                entity.cw_etype,
                'service',
                'creation',
                'OAIRepository',
            ),
            getRelatedUiSchema(entity.cw_etype, 'service', 'OAIRepository'),
            getRelated(entity.cw_etype, entity.eid, 'service', {
                sort: 'url',
                targetType: 'OAIRepository',
            }),
        ]).then(([schema, uiSchema, related]) => {
            setSchema(schema)
            setUiSchema(uiSchema)
            setRelated(related)
            setDisplayCreationForm(related.length === 0)
            setLoading(false)
        })
    }, [entity])

    function handleSubmitCreate(formData) {
        const {eid, cw_etype} = entity
        return relateEntity(
            cw_etype,
            eid,
            'service',
            formData,
            'OAIRepository',
        ).then(formRedirects.onSubmit)
    }

    function renderCreationForm() {
        let body = ce(CmsForm, {
            schema,
            uiSchema,
            onCancel: formRedirects.onCancel,
            formData: {},
            FieldTemplate: CustomFieldTemplateConnected,
            onSubmit: ({formData}) => handleSubmitCreate(formData),
        })
        return ce(
            'div',
            {className: 'panel panel-default'},
            ce('div', {className: 'panel-body'}, body),
        )
    }

    // prevent rendering when data have not been loaded yet.
    if (typeof schema === 'undefined' || loading) {
        return ce(spinner)
    }
    let creationForm = displayCreationForm ? renderCreationForm() : null
    return (
        <div>
            <EntityRelatedEditor.Header
                entityTitle={entity.dc_title}
                title="entrepôt OAI"
                onAddClick={() => setDisplayCreationForm(true)}
            />
            <div className="related-entities">
                {related.map((e) => (
                    <OAIRepositoryEditForm
                        key={e.eid}
                        entity={e}
                        schema={schema}
                        uiSchema={uiSchema}
                        formRedirects={formRedirects}
                        dispatch={dispatch}
                    />
                ))}
            </div>
            {creationForm}
        </div>
    )
}

OAIRelatedEditor.propTypes = {
    entity: PropTypes.object,
    dispatch: PropTypes.func.isRequired,
    formRedirects: PropTypes.shape({
        onCancel: PropTypes.func,
        onSubmit: PropTypes.func,
    }).isRequired,
}

class ServiceListEditor extends Component {
    constructor(props, ctx) {
        super(props, ctx)
        this.state = {data: null, selectedService: null}
        this.updateData = this.updateData.bind(this)
        this.handleClick = this.handleClick.bind(this)
    }

    componentDidMount() {
        jsonFetch(getJsonUrl()).then((data) => this.setState({data: data.data}))
        this.bindClickMap()
    }

    bindClickMap() {
        this._map = $('#dpt-vmap')
        if (this._map) {
            this._map.on('click', 'path', this.updateData)
        }
    }

    updateData() {
        this.setState({data: null})
        jsonFetch(getJsonUrl()).then((data) => this.setState({data: data.data}))
    }

    unbindClickMap() {
        if (this._map) {
            this._map.off('click', 'path', this.updateData)
            this._map = null
        }
    }

    componentWillUnmount() {
        this.unbindClickMap()
    }

    handleClick() {
        this.setState({selectedService: null, formType: null})
    }

    render() {
        const {data, selectedService, formType} = this.state
        const body =
            data === null
                ? ce(spinner)
                : ce(
                      BT,
                      {
                          data,
                          striped: true,
                          hover: true,
                          search: true,
                          pagination: true,
                      },
                      ce(
                          THC,
                          {dataField: 'eid', isKey: true, hidden: true},
                          'eid',
                      ),
                      ce(
                          THC,
                          {
                              dataFormat: (cell, service) =>
                                  ce(
                                      'div',
                                      null,
                                      ce(button, {
                                          title: 'éditer le service',
                                          onClick: () =>
                                              this.setState({
                                                  selectedService: service,
                                                  formType: 'edit',
                                              }),
                                          name: 'edit',
                                      }),
                                      ce(button, {
                                          title: 'changer le logo du service',
                                          onClick: () =>
                                              this.setState({
                                                  selectedService: service,
                                                  formType: 'image',
                                              }),
                                          name: 'image',
                                      }),
                                      ce(button, {
                                          title: 'éditer/ajouter un entrepôt OAI',
                                          onClick: () =>
                                              this.setState({
                                                  selectedService: service,
                                                  formType: 'oai',
                                              }),
                                          name: 'database',
                                      }),
                                      ce(button, {
                                          title: 'définir ce service comme une annexe',
                                          onClick: () =>
                                              this.setState({
                                                  selectedService: service,
                                                  formType: 'annex',
                                              }),
                                          name: 'link',
                                      }),
                                      ce(button, {
                                          title: 'supprimer ce service',
                                          onClick: () =>
                                              this.setState({
                                                  selectedService: service,
                                                  formType: 'delete',
                                              }),
                                          name: 'trash',
                                      }),
                                  ),
                          },
                          'outils',
                      ),
                      ce(THC, {dataField: 'dc_title', dataSort: true}, 'Nom'),
                      ce(THC, {dataField: 'address'}, 'Addresse'),
                      ce(THC, {dataField: 'city'}, 'Ville'),
                  )
        let form = null
        if (formType === 'edit') {
            form = ce(ServiceEditor, {
                entity: selectedService,
                dispatch: this.props.dispatch,
            })
        } else if (formType === 'image') {
            const rtype = 'service_image'
            form = ce(EntityRelatedEditor.EntityRelatedEditor, {
                entity: Immutable.fromJS(selectedService),
                rtypes: Immutable.fromJS({
                    service_image: {
                        rtype: rtype,
                        title: 'image de service',
                    },
                }),
                dispatch: this.props.dispatch,
                location: {search: `?name=${rtype}`},
                formRedirects: {
                    onCancel: this.handleClick,
                    onSubmit: this.handleClick,
                },
            })
        } else if (formType === 'oai') {
            form = ce(OAIRelatedEditor, {
                entity: selectedService,
                dispatch: this.props.dispatch,
                formRedirects: {
                    onCancel: this.handleClick,
                    onSubmit: this.handleClick,
                },
            })
        } else if (formType === 'delete') {
            form = ce(DeleteForm, {
                entity: Immutable.fromJS(selectedService),
                location: {query: {}},
            })
        } else if (formType === 'annex') {
            form = ce(SelectAnnex, {service: selectedService})
        }
        return ce(
            'div',
            null,
            ce('h1', null, "Édition de l'annuaire de service"),
            body,
            form,
        )
    }
}

ServiceListEditor.propTypes = {
    dispatch: PropTypes.func.isRequired,
}

exports.ServiceListEditor = connect()(ServiceListEditor)

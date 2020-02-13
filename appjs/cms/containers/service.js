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

const {Component, createElement: ce} = require('react'),
      PropTypes = require('prop-types'),
      Select = require('react-select'),
      {connect} = require('react-redux'),
      _  = require('lodash'),
      Immutable = require('immutable'),
      {BootstrapTable: BT, TableHeaderColumn: THC} = require('react-bootstrap-table');

const {default: {
    jsonFetch,
    getSchema,
    createEntity,
    getUiSchema,
    updateEntity,
    getEntity,
    getRelated,
    addRelation,
    deleteRelation,
    getAvailableTargets,
}} = require('../api');
const {buildFormData} = require('../utils');
const {spinner, icon, button} = require('../components/fa'),
      {showErrors} = require('../actions'),
      {CmsForm} = require('../components/editor'),
      DeleteForm = require('../components/delete'),
      {EntityRelatedEditor} = require('../components/relatededitor');

const {CustomFieldTemplateConnected} = require('./form');


class SelectAnnex extends Component {
    constructor(props) {
        super(props);
        this.state = {annex: null, loading: true, bodyType: null};
        this.validate = this.validate.bind(this);
        this.deleteAnnex = this.deleteAnnex.bind(this);
    }

    UNSAFE_componentWillReceiveProps(nextProps) {
        if (this.props.service.eid === nextProps.service.eid) {
            return;
        }
        this.fetchData(nextProps.service);
    }

    fetchData(service) {
        this.setState({loading: true, annex: null, bodyType: null});
        getRelated('service', service.eid, 'annex_of')
            .then(services => {
                if (this.unmounted) {
                    // do not call setState on an unmounted Component
                    return;
                }
                if (services.length) {
                    this.setState({annex: {label: services[0].dc_title,
                                           value: services[0].eid}});
                } else {
                    this.setState({title: null});
                }
                this.setState({loading: false});
            })
    }

    componentDidMount() {
        this.fetchData(this.props.service)
    }

    componentWillUnmount() {
        this.unmounted = true;
    }

    validate() {
        this.setState({bodyType: null});
        const {service} = this.props,
              {annex} = this.state;
        if (annex) {
            return addRelation('service', service.eid, 'annex_of',
                               [annex]);
        } else {
            return deleteRelation('service', service.eid, 'annex_of');
        }
    }

    deleteAnnex() {
        this.setState({bodyType: null, annex: null});
        const {service} = this.props;
        return deleteRelation('service', service.eid, 'annex_of');
    }

    render() {
        const {loading, annex, bodyType} = this.state,
              {service} = this.props;
        let body;

        function loadOptions(input) {
            if (input.length < 3) {
                return Promise.resolve({complete: false});
            }
            return getAvailableTargets('service', 'annex_of', service.eid, input)
                .then(d => ({
                    complete: true,
                    options: d.map(e => ({label: e.title, value: e.eid})),
                }))
        }

        if (loading) {
            body = ce(spinner);
        } else if (bodyType === 'select') {
            body = ce('div', null,
                      ce(Select.Async, {
                          loadOptions: _.throttle(loadOptions, 300),
                          value: annex,
                          onChange: value => this.setState({annex: value}),
                      }),
                      ce('button', {className: 'btn-default btn',
                                    onClick: () => this.setState({bodyType: null})},
                         'annuler'),
                      ce('button', {className: 'btn-primary btn',
                                    onClick: this.validate}, 'valider'));
        } else if (bodyType === 'delete') {
            body = ce('div', null,
                      'voulez supprimer ce lien annexe ? ',
                      ce('button', {className: 'btn-default btn',
                                   onClick: () => this.setState({bodyType: null})},
                         'annuler'),
                      ce('button', {className: 'btn-primary btn',
                                    onClick: this.deleteAnnex},
                         'valider'));
        } else if (bodyType === null && annex !== null) {
            body = ce('span', null,
                      annex.label, ' ',
                      ce('button', {className: 'btn btn-link',
                                    onClick: () => this.setState({bodyType: 'select'})},
                         'modifier'),
                      ce('button', {className: 'btn btn-link',
                                    onClick: () => this.setState({bodyType: 'delete'})},
                         'supprimer le lien'));
        } else {
            body = ce('div', null,
                      ce('i', null, 'pas de service relié'),
                      ' ',
                      ce('button', {className: 'btn btn-link',
                                 onClick: () => this.setState({bodyType: 'select'})},
                      'modifier'));
        }
        return ce('div', null,
                  ce('h2', null,
                     `Le service "${service.dc_title}" est une annexe de :`),
                  body);
    }
}
SelectAnnex.propTypes = {
    service: PropTypes.object.isRequired,
};



class AddService extends Component {
    constructor(props) {
        super(props);
        this.state = {schema: null, uiSchema: null, formData:null};
        this.onSubmit = this.onSubmit.bind(this);
    }

    onSubmit(ev) {
        this.setState({formData: ev.formData});
        return createEntity('service', ev.formData)
            .then(doc => {
                if (doc.errors && doc.errors.length) {
                    this.props.dispatch(showErrors(doc.errors));
                    return;
                } else if (doc.absoluteUrl) {
                    document.location.replace(doc.absoluteUrl);
                }
            });
    }

    componentDidMount() {
        Promise.all([
            getSchema('service', null, 'creation'),
            getUiSchema('service'),
        ]).then(([schema, uiSchema]) => this.setState({schema, uiSchema}));
    }

    render() {
        const {schema, uiSchema, formData} = this.state;
        let body;
        if (schema === null) {
            body = ce(spinner);
        } else {
            body = ce(CmsForm, {schema,
                                uiSchema,
                                formData,
                                FieldTemplate: CustomFieldTemplateConnected,
                                onSubmit: this.onSubmit});
        }
        return ce('div', null,
                  ce('h1', null, 'Ajouter un nouveau service'),
                  body);
    }
}

AddService.propTypes = {
    dispatch: PropTypes.func.isRequired,
};

exports.AddService = connect()(AddService);





class ServiceEditor extends Component {
    constructor(props) {
        super(props);
        this.state = {formData: null, schema: null,
                      uiSchema: null, loadingEditor: true};
        this.onEditSubmit = this.onEditSubmit.bind(this);
    }

    UNSAFE_componentWillReceiveProps(nextProps) {
        if (nextProps.entity.eid === this.props.entity.eid) {
            return;
        }
        this.fetchData(nextProps.entity.eid);
    }

    fetchData(eid) {
        this.setState({loadingEditor: true, eid});
        const promises = [
            getEntity('service', eid),
            getSchema('service', eid, 'edition'),
        ];
        if (this.state.uiSchema) {
            promises.push(Promise.resolve(this.state.uiSchema));
        } else {
            promises.push(getUiSchema('service'));
        }
        Promise.all(promises).then(
            ([entity, schema, uiSchema]) => this.setState(
                {uiSchema, formData: entity, schema, loadingEditor: false}));
    }

    componentDidMount() {
        this.fetchData(this.props.entity.eid);
    }

    onEditSubmit(ev) {
        const {eid} = this.props.entity;
        this.setState({formData: ev.formData});
        return updateEntity('service', eid, ev.formData)
            .then(doc => {
                if (doc.errors && doc.errors.length) {
                    this.props.dispatch(showErrors(doc.errors));
                    return;
                }
                document.location.reload();
            });
    }

    render() {
        const {loadingEditor, schema, uiSchema, formData} = this.state;
        let form;
        if (loadingEditor) {
            form = ce(spinner);
        } else {
            form = ce(CmsForm, {schema,
                                uiSchema,
                                formData: buildFormData(formData, schema),
                                FieldTemplate: CustomFieldTemplateConnected,
                                onSubmit: this.onEditSubmit});
        }
        return ce('div', null,
                  ce('h1', null, "Édition de l'annuaire de service"),
                  form);
    }
}

ServiceEditor.propTypes = {
    dispatch: PropTypes.func.isRequired,
    entity: PropTypes.object.isRequired,
};


exports.ServiceEditor = connect()(ServiceEditor);


function getJsonUrl() {
    const pathname = document.location.pathname;
    const isdpt = pathname.includes('departements'),
          segments = pathname.split('/'),
          url = isdpt ? '/annuaire/departements' + document.location.search :
              '/annuaire/' + segments[segments.length - 1];
    return url;
}

class OAIPublishButton extends Component {
    constructor(props) {
        super(props);
        this.createTask = this.createTask.bind(this);
        this.state = {loading: false};
    }

    createTask() {
        const {eid} = this.props.entity;
        this.setState({loading: true});
        return createEntity(
            'RqTask',
            {
                name: 'import_oai',
                title: `import-oai for OAIRepository #${eid}`,
                oairepository: eid,
            },
            'import_oai'
        ).then(doc => {
            if (doc.errors && doc.errors.length) {
                this.props.dispatch(showErrors(doc.errors));
                return;
            } else if (doc.absoluteUrl) {
                document.location.replace(doc.absoluteUrl);
            }
        });
    }

    render() {
        let iconComp;
        if (this.state.loading) {
            iconComp = ce(spinner);
        } else {
            iconComp = ce(icon, {name: 'play'});
        }
        return ce('button', {
            className: 'btn btn-default pull-right',
            title: 'moissonner cet entrepôt',
            onClick: this.createTask,
        }, iconComp, ' moissonner');
    }
}

OAIPublishButton.propTypes = {
    dispatch: PropTypes.func.isRequired,
    entity: PropTypes.shape({
        eid: PropTypes.integer,
    }).isRequired,
};


class OAIRelatedEditor extends EntityRelatedEditor {
    entityFormOtherButtons(entity) {
        const components = super.entityFormOtherButtons(entity);
        return [
            ce(OAIPublishButton, {entity, dispatch: this.props.dispatch}),
            ...components,
        ];
    }
}


class ServiceListEditor extends Component {
    constructor(props, ctx) {
        super(props, ctx);
        this.state = {data: null, selectedService: null};
        this.updateData = this.updateData.bind(this);
        this.handleClick = this.handleClick.bind(this);
    }

    componentDidMount() {
        jsonFetch(getJsonUrl()).then(data => this.setState({data: data.data}));
        this.bindClickMap();
    }

    bindClickMap() {
        this._map = $('#dpt-vmap');
        if (this._map) {
            this._map.on('click', 'path', this.updateData);
        }
    }

    updateData() {
        this.setState({data: null});
        jsonFetch(getJsonUrl())
            .then(data => this.setState({data: data.data}));
    }

    unbindClickMap() {
        if (this._map) {
            this._map.off('click', 'path', this.updateData);
            this._map = null;
        }
    }

    componentWillUnmount() {
        this.unbindClickMap();
    }

    handleClick() {
        this.setState({selectedService: null, formType: null});
    }

    render() {
        const {
            data,
            selectedService,
            formType,
        } = this.state;
        const body = data === null ? ce(spinner) :
                  ce(BT, {data, striped:true,
                          hover:true,
                          search: true,
                          pagination: true,
                         },
                     ce(THC, {dataField:"eid", isKey:true, hidden: true}, 'eid'),
                     ce(THC, {
                         dataFormat: (cell, service) => ce(
                             'div', null,
                             ce(button, {
                                 title: 'éditer le service',
                                 onClick: () => this.setState(
                                     {selectedService: service,
                                      formType: 'edit'}),
                                 name: 'edit'}),
                             ce(button, {
                                 title: 'changer le logo du service',
                                 onClick: () => this.setState(
                                     {selectedService: service,
                                      formType: 'image'}),
                                 name: 'image'}),
                             ce(button, {
                                 title: 'éditer/ajouter un entrepôt OAI',
                                 onClick: () => this.setState(
                                     {selectedService: service,
                                      formType: 'oai'}),
                                 name: 'database'}),
                             ce(button, {
                                 title: 'définir ce service comme une annexe',
                                 onClick: () => this.setState(
                                     {selectedService: service,
                                      formType: 'annex'}),
                                 name: 'link'}),
                             ce(button, {
                                 title: 'supprimer ce service',
                                 onClick: () => this.setState(
                                     {selectedService: service, formType: 'delete'}),
                                 name: 'trash'})),
                         }, 'outils'),
                     ce(THC, {dataField: "dc_title", dataSort:true}, 'Nom'),
                     ce(THC, {dataField: "address"}, 'Addresse'),
                     ce(THC, {dataField: "city"}, 'Ville'));
        let form = null;
        if (formType === 'edit') {
            form = ce(ServiceEditor, {entity: selectedService,
                                      dispatch: this.props.dispatch});
        } else if (formType === 'image') {
            const rtype = 'service_image';
            form = ce(EntityRelatedEditor, {
                entity: Immutable.fromJS(selectedService),
                rtypes:Immutable.fromJS(
                    {service_image: {
                        rtype: rtype, title: 'image de service'}}),
                dispatch: this.props.dispatch,
                location: {search: `?name=${rtype}`},
                formRedirects: {onCancel: this.handleClick,
                                onSubmit: this.handleClick},
            });
        } else if (formType === 'oai') {
            const rtype = 'service';
            form = ce(OAIRelatedEditor, {
                targetType: 'OAIRepository',
                entity: Immutable.fromJS(selectedService),
                rtypes:Immutable.fromJS(
                    {[rtype]: {
                        rtype: rtype, title: 'entrepôt OAI'}}),
                dispatch: this.props.dispatch,
                location: {search: `?name=${rtype}`},
                sortTerm: 'url',
                formRedirects: {onCancel: this.handleClick,
                                onSubmit: this.handleClick},
            });
        } else if (formType === 'delete') {
            form = ce(DeleteForm, {entity: Immutable.fromJS(selectedService),
                                   location: {query: {}}});
        } else if (formType === 'annex') {
            form = ce(SelectAnnex, {service: selectedService});
        }
        return ce('div', null,
                  ce('h1',
                     null,
                     "Édition de l'annuaire de service"),
                  body,
                  form);
    }
}

ServiceListEditor.propTypes = {
    dispatch: PropTypes.func.isRequired,
};


exports.ServiceListEditor = connect()(ServiceListEditor);

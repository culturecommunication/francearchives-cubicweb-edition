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

const {Component, createElement: ce, PropTypes} = require('react'),
      Select = require('react-select'),
      _ = require('lodash'),
      {Link} = require('react-router');


const {CmsForm} = require('./editor'),
      {spinner, icon} = require('./fa');

const {CustomFieldTemplateConnected} = require('../containers/form');

const {default: {
    getRelated,
    getRelatedSchema,
    getRelatedUiSchema,
    relateEntity,
    getAvailableTargets,
    addRelation,
    updateEntity,
}} = require('../api');
const {buildFormData} = require('../utils');


const location_reload = document.location.reload.bind(document.location);


class EntityRelatedEditor extends Component {
    constructor(props) {
        super(props);
        this.onSubmit = this.onSubmit.bind(this);
        this.sendTargets = this.sendTargets.bind(this);
        this.formRedirects = this.props.formRedirects || this.initFormRedirects();
        this.rtypes = props.rtypes.toJS();
        this.state = this.initState(props);
    }

    initFormRedirects() {
        const location_reload = document.location.reload.bind(document.location);
        return {onCancel: location_reload,
                onSubmit: location_reload};

    }

    initState(props) {
        const rtype = props.location.query.name,
              targetType = props.targetType || null,
              sortTerm = props.sortTerm || null,
              nextEntity = props.entity.toJS(),
              nextState =  {rtype};
        if (this.state !== undefined) {
            // this.state === undefined means we come from constructor
            if (this.state.rtype === rtype &&
                nextEntity.cw_etype === this.entity.cw_etype &&
                nextEntity.eid === this.entity.eid) {
                // if rtype and entity have not changed do not fetch related
                // entities and schema
                return nextState;
            }
        }
        // unset ``displayCreationForm`` if rtype has changed
        nextState.displayCreationForm = false;
        nextState.loading = true;
        nextState.title = this.rtypes[rtype].title;
        nextState.multiple = this.rtypes[rtype].multiple;
        nextState.fetchPossibleTargets = this.rtypes[rtype].fetchPossibleTargets;
        this.entity = nextEntity;
        Promise.all([getRelatedSchema(this.entity.cw_etype, rtype, 'creation', targetType),
                     getRelatedUiSchema(this.entity.cw_etype, rtype, targetType),
                     getRelated(this.entity.cw_etype, this.entity.eid, rtype,
                                {sort: sortTerm})])
            .then(([schema, uiSchema, related]) => {
                const targets = related.map(
                    r => ({value: r.eid, label: r.dc_title}));
                this.setState(
                    {schema, uiSchema, related,
                     loading: false,
                     targets: nextState.multiple ? targets : targets[0]});
            });

        return nextState;
    }

    componentWillReceiveProps(nextProps) {
        this.setState(this.initState(nextProps));
    }

    onSubmit(entity, create, {formData}) {
        let res;
        if (create) {
            const rtype = this.props.location.query.name;
            res = this.createEntity(entity, formData, rtype)
        } else {
            res = this.editEntity(entity, formData);
        }
        return res.then(this.formRedirects.onSubmit);
    }

    createEntity(entity, formData, rtype) {
        const {eid, cw_etype} = entity,
              targetType = this.props.targetType || null;
        return relateEntity(cw_etype, eid, rtype, formData, targetType);
    }

    editEntity(entity, formData) {
        return updateEntity(entity.cw_etype, entity.eid, formData);
    }

    entityFormOtherButtons(entity) {
        return [
            ce(Link, {
                to : {pathname: '/delete',
                      query: {eid:entity.eid, cwetype:entity.cw_etype}},
                className: 'btn btn-default pull-right'},
               'supprimer'),
        ];
    }

    renderEntityForm(e) {
        const {schema, uiSchema} = this.state;
        const formData = buildFormData(e, schema);
        return ce('div', {className: "panel panel-default", key: e.eid},
                  ce('div', {className: "panel panel-heading"},
                     ce('div', {className: "panel-title"}, e.dc_title)),
                  ce('div', {className: 'panel-body'},
                     schema
                     ? ce(CmsForm, {schema,
                                    uiSchema,
                                    onCancel: this.formRedirects.onCancel,
                                    formData: formData,
                                    FieldTemplate: CustomFieldTemplateConnected,
                                    onSubmit: this.onSubmit.bind(this, e, false)},
                          ce('div', {className: 'btn-group'},
                             ce('button', {
                                 'type': 'button',
                                 onClick: () => document.location.reload(),
                                 className: 'btn btn-default'}, 'annuler'),
                             ce('button', {type: 'submit', className: 'btn btn-primary'},
                                'enregistrer')),
                          ...this.entityFormOtherButtons(e)
                          )
                     : ce(spinner)));
    }

    sendTargets(e) {
        e.preventDefault();
        const {cw_etype, eid} = this.entity,
              {rtype, targets} = this.state;
        return addRelation(
            cw_etype, eid, rtype, Array.isArray(targets) ? targets : [targets]
        ).then(this.formRedirects.onSubmit);
    }

    displayCreationForm() {
        const {schema, uiSchema} = this.state;
        return ce(
            CmsForm,
            {schema,
             uiSchema,
             onCancel: this.formRedirects.onCancel,
             formData: {},
             FieldTemplate: CustomFieldTemplateConnected,
             onSubmit: this.onSubmit.bind(this,
                                          this.entity,
                                          true)});
    }

    displayTargetsSearch() {
        const {title, rtype, multiple} = this.state,
              {entity} = this;
        function loadOptions(input) {
                if (input.length < 3) {
                    return Promise.resolve({complete: false});
                }
                return getAvailableTargets(entity.cw_etype, rtype, entity.eid, input)
                    .then(d => ({
                        complete: true,
                        options: d.map(e => ({label: e.title, value: e.eid})),
                    }))
            }
            return ce('div', null,
                      ce('h1', null, title),
                      ce('form', {onSubmit: this.sendTargets},
                         ce(Select.Async, {
                             multi: multiple,
                             name: 'targets',
                             ignoreAccents: false,
                             loadOptions: _.throttle(loadOptions, 300),
                             value: this.state.targets,
                             onChange: value => this.setState({targets: value}),
                         }),
                         ce('button', {type: 'button',
                                       className: 'btn btn-default',
                                       onClick: this.formRedirects.onCancel},
                            'annuler'),
                         ce('button', {className: 'btn btn-primary',type: 'submit'}, 'envoyer')));
    }

    displayRelatedEntities() {
        const {related} = this.state;
        return ce('div', {className: "related-entities"},
                     related ? related.map(this.renderEntityForm, this) : ce(spinner));
    }

    displayFormHeader() {
        const {title} = this.state;
        return ce('h1', null,
                     title,
                     ce('button', {className: 'btn',
                                   onClick: () => this.setState({displayCreationForm: true})},
                        '+'));
    }

    render() {
        // prevent rendering when data have not been loaded yet.
        const {loading, displayCreationForm} = this.state;
        if (typeof this.state.schema === 'undefined' || loading) {
            return ce(spinner);
        }
        const {fetchPossibleTargets} = this.state;
        if (fetchPossibleTargets) {
            return this.displayTargetsSearch();
        }
        let creationForm = displayCreationForm ? this.displayCreationForm(): null;
        return ce('div', null,
                  this.displayFormHeader(),
                  this.displayRelatedEntities(),
                  creationForm);
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
};


exports.EntityRelatedEditor = EntityRelatedEditor;


class IndexEntityRelatedEditor extends EntityRelatedEditor{

    initState(props) {
        const rtype = props.location.query.name,
              nextEntity = props.entity.toJS(),
              etarget = props.location.query.etarget || null,
              nextState =  {rtype};

        if (this.state !== undefined) {
            // this.state === undefined means we come from constructor
            if (this.state.rtype === rtype &&
                nextEntity.cw_etype === this.entity.cw_etype &&
                nextEntity.eid === this.entity.eid) {
                // if rtype and entity have not changed do not fetch related
                // entities and schema
                return nextState;
            }
        }
        nextState.displayCreationForm = false;
        nextState.title = this.rtypes[rtype].title;
        nextState.multiple = this.rtypes[rtype].multiple;
        nextState.fetchPossibleTargets = this.rtypes[rtype].fetchPossibleTargets;
        nextState.etarget = etarget;
        nextState.indexes = null;
        nextState.indexesSearchOption = rtype === 'index_agent' ? 'persname' :null;
        nextState.loading = true;
        this.entity = nextEntity;
        Promise.all([getRelatedSchema(this.entity.cw_etype, rtype),
                     getRelatedUiSchema(this.entity.cw_etype, rtype),
                     getRelated(this.entity.cw_etype, this.entity.eid, rtype, {etargetType:etarget})])
            .then(([schema, uiSchema, related]) => {
                const targets = related.map(
                    r => ({value: r.eid, label: r.dc_title}));
                this.setState(
                    {schema, uiSchema, related,
                     etarget: etarget,
                     loading: false,
                     targets: nextState.multiple ? targets : targets[0]});
            });

        return nextState;
    }

    sendIndexes(e) {
        e.preventDefault();
        const {cw_etype, eid} = this.entity,
              {targets, indexes, rtype} = this.state,
              allindexes = [];
        if (Array.isArray(indexes)) {
            allindexes.push(...indexes)
        } else {
            allindexes.push(indexes)
        }
        if (Array.isArray(targets)) {
            allindexes.push(...targets)
        } else {
            allindexes.push(targets)
        }
        return addRelation(
            cw_etype, eid, rtype, allindexes
        ).then(location_reload);
    }

    displayIndexesSearch() {
        const {entity} = this,
              {etarget, rtype, indexesSearchOption} = this.state;
        function loadOptions(input) {
            if (input.length < 2) {
                return Promise.resolve({complete: false});
            }
            const params = {};
            if (indexesSearchOption !== null) {
                params.t = indexesSearchOption;
            }
            return getAvailableTargets(
                entity.cw_etype, rtype, entity.eid, input, params
            ).then(d => ({
                complete: true,
                options: d.map(e => ({label: e.title, value: e.eid})),
            }))
        }

        let select = null;
        if (etarget === 'PniaAgent') {
            const options = ['persname', 'corpname', 'famname', 'name'];
            select = ce('div', {className: 'form-group'},
                        ce('label', {className: 'control-label required'}, 'type'),
                        ce('select', {name:'type',
                                      className: 'form-control',
                                      value: 'persname',
                                      onChange: e => this.setState(
                                          {indexesSearchOption: e.target.value,
                                           indexes: null}),
                                     },
                           options.map(value => ce('option', {value: value, key:'at'+value}, value ))));
        }
        return ce('div', {className: 'formBlock'},
                  ce('form', {onSubmit: this.sendIndexes.bind(this)},
                     select,
                     ce('div', {className: 'form-group'},
                        ce('label', {className: 'control-label required'}, 'index'),
                        ce(Select.Async, {
                            multi: true,
                            name: 'targets',
                            ignoreAccents: false,
                            loadOptions: _.throttle(loadOptions, 300),
                            value: this.state.indexes,
                            onChange: value => this.setState({indexes: value}),
                        })),
                     ce('button', {className: 'btn btn-default',
                                   onClick: location_reload},
                        'annuler'),
                     ce('button', {className: 'btn btn-primary',type: 'submit'}, 'envoyer')));
    }

    deleteIndex(indexEid) {
        const {rtype, related, targets} = this.state,
              {cw_etype, eid} = this.entity;
        _.remove(related, el => el.eid === indexEid);
        _.remove(targets, el => el.value === indexEid);
        const allindexes = new Set(
            related.map(r => r.eid).concat(
                targets.map(t => t.value)
            )
        );
        return addRelation(
            cw_etype, eid, rtype,
            Array.from(allindexes).map(eid => ({value: eid}))
        ).then(() => this.setState({related, targets})).catch(e => console.error(e));
    }

    renderEntityForm(e) {
        const {schema} = this.state;
        const formData = buildFormData(e, schema);
        let body = schema ?
            ce('div', {style: {position: 'relative'}},
               ce(icon, {className: 'pointer',
                         name: 'times', style: {position: 'absolute', right: '1em'},
                         onClick: this.deleteIndex.bind(this, e.eid)}),
                   ce('div', null,
                      ce('span', null, ce('strong', null, 'libellé : ')),
                      ce('span', null, formData.label)),
                   formData.type !== 'undefined' && formData.type?
                      ce('div', null,
                         ce('span', null, ce('strong', null, 'type : ')),
                         ce('span', null , formData.type))
                     : null
                   )
            : ce(spinner);
        return ce('div', {className: "panel panel-default", key: e.eid},
                  ce('div', {className: 'panel-body'},
                     body));
    }

   render() {
        // prevent rendering when data have not been loaded yet.
        const {loading} = this.state;
        if (typeof this.state.schema === 'undefined' || loading) {
            return ce(spinner);
        }
        return ce('div', null,
                  this.displayFormHeader(),
                  this.displayIndexesSearch(),
                  ce('h3', null, 'Index liés'),
                  this.displayRelatedEntities());
    }
}

exports.IndexEntityRelatedEditor = IndexEntityRelatedEditor;


class CssImageEntityRelatedEditor extends EntityRelatedEditor{
    entityFormOtherButtons() {
        return [
        ];
    }

}

exports.CssImageEntityRelatedEditor = CssImageEntityRelatedEditor;

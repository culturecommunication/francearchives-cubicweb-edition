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

const {createElement: ce, Component} = require('react'),
      PropTypes = require('prop-types');


const {CustomFieldTemplateConnected} = require('../containers/form'),
      {spinner} = require('./fa'),
      {CmsForm} = require('./editor');

const {default: {
    getSchema,
    getUiSchema,
    relateEntity,
    jsonSchemaFetch,
}} = require('../api');


function SelectContentType({onChange, options}) {
    return ce('select', {onChange: ev => onChange(ev.target.value),
                         defaultValue: options[0][0],
                         className: 'form-control'},
              options.map(([etype, label], i) => ce(
                  'option', {key: `opt-${i}`, value: etype}, label)));
}


class Add extends Component {
    constructor(props, context) {
        super(props, context);
        this.state = {schema: null, uiSchema: null, etype: null, options: null};
        this.entity = this.props.entity.toJS();
        this.updateSelected = this.updateSelected.bind(this);
        this.onSubmit = this.onSubmit.bind(this);
        this.etype2href = {};
    }

    componentDidMount() {
        // fetch schema for current entity and then all targetSchema in
        // `related.children` links to build data for SelectContentType
        // component
        const {cw_etype, eid} = this.entity;
        getSchema(cw_etype, eid).then(schema => {
            const links = schema.links.filter(l => l.rel === 'related.children').sort((a, b) => a.order - b.order),
                  options = [];
            for (let link of links) {
                this.etype2href[link.etype] = link.targetSchema.$ref;
                options.push([link.etype, link.description]);
            }
            this.setState({options});
            this.updateSelected(options[0][0]);
        });
    }

    updateSelected(etype) {
        this.setState({schema: null, uiSchema: null, etype});
        Promise.all([
            getUiSchema(etype),
            jsonSchemaFetch(this.etype2href[etype]),
        ]).then(([uiSchema, schema]) => this.setState({schema, uiSchema}));
    }

    onSubmit(ev) {
        const {cw_etype, eid} = this.entity;
        return relateEntity(cw_etype, eid, 'children', ev.formData, this.state.etype)
            .then(doc => {
                if (doc.errors && doc.errors.length) {
                    this.props.showErrors(doc.errors);
                } else if (doc.absoluteUrl || doc.cwuri) {
                    document.location.replace(doc.absoluteUrl || doc.cwuri);
                }
            });
    }

    render() {
        const {etype, schema, uiSchema, options} = this.state,
              {errors} = this.props;
        return ce('div', null,
                  ce('span', null, 'Ajouter du contenu dans cette rubrique :'),
                  options === null ? ce(spinner) : ce(SelectContentType, {
                      options, onChange: this.updateSelected}),
                  schema ?
                  ce(CmsForm, {schema,
                               uiSchema,
                               onSubmit: this.onSubmit,
                               serverErrors: errors,
                               formContext: {'cw_etype': etype},
                               FieldTemplate: CustomFieldTemplateConnected})
                  : ce(spinner));
    }
}

Add.propTypes = {
    entity: PropTypes.object.isRequired,
    errors: PropTypes.object.isRequired,
    showErrors: PropTypes.func.isRequired,
}

module.exports = Add;

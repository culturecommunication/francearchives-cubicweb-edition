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
import React from 'react';
import {connect} from 'react-redux';
import PropTypes from 'prop-types';

import Api from '../api';

function Spinner() {
    return <i className="fa fa-spin fa-spinner"></i>;
}


class SameAsItem extends React.Component {
    constructor(props) {
        super(props);
        this.onUriChange = this.onUriChange.bind(this);
        this.onLabelChange = this.onLabelChange.bind(this);
        this.onDelete = this.onDelete.bind(this);
    }

    updateAttr(name, value) {
        const partialProps = {[name]: value};
        this.props.onChange(Object.assign({}, this.props.item, partialProps));
    }

    onLabelChange(ev) {
        ev.preventDefault();
        this.updateAttr('label', ev.target.value);
    }

    onUriChange(ev) {
        ev.preventDefault();
        this.updateAttr('uri', ev.target.value);
    }

    onDelete(ev) {
        ev.preventDefault();
        this.updateAttr('toDelete', true);
    }

    render() {
        const {label, uri, source, latitude, longitude, link} = this.props.item;
        let uriInput, coordinatesIntput=null, sourceInput=null;
        if (uri === undefined) { /* ExternalId entity */
            uriInput = <input className="form-control" value={uri} disabled="disabled" />;
        }
        else {
            uriInput = <input className="form-control" value={uri} onChange={this.onUriChange} />;
        }
        if (source !== undefined) { /* new form*/
            sourceInput = (
                <div className="form-group">
                    <label className="control-label col-xs-1">source</label>
                    <div className="col-xs-11">
                        <input className="form-control" value={source} disabled="disabled"  />
                    </div>
                </div>);
        }
        if (latitude && longitude) {
           let mapUri = uri || link;
           coordinatesIntput = (
            <div>
               <div className="form-group">
                    <label className="control-label col-xs-1">latitude</label>
                    <div className="col-xs-4">
                        <input className="form-control" value={latitude} disabled="disabled" />
                    </div>
                    <label className="control-label col-xs-1">longitude</label>
                    <div className="col-xs-4">
                        <input className="form-control" value={longitude} disabled="disabled" />
                    </div>
                   <label className="control-label col-xs-1">voir la carte</label>
                       <a href={mapUri} className="fa-stack fa-lg url_link col-xs-2"
                           target="_blank"
                           rel="noopener noreferrer">
                           <i className="fa fa-circle fa-stack-2x"></i>
                           <i className="fa fa-arrow-right  fa-stack-1x fa-inverse"></i>
                       </a>
                </div>
            </div>
            );
        }
        return (
            <div style={{position: 'relative'}}>
                <button className="btn btn-danger" onClick={this.onDelete} style={{position: 'absolute', right: '0.1em'}}>
                    <i className="fa-times fa"></i>
                </button>
                <fieldset>
                  <legend>Alignement</legend>
                  <div className="form-group">
                    <label className="control-label col-xs-1">libéllé</label>
                    <div className="col-xs-11">
                        <input className="form-control" value={label} onChange={this.onLabelChange} />
                    </div>
                  </div>
                <div className="form-group">
                    <label className="control-label col-xs-1">uri</label>
                    <div className="col-xs-11">{ uriInput }</div>
                </div>
                {coordinatesIntput}
                {sourceInput}
              </fieldset>
            </div>
        );
    }
}

SameAsItem.propTypes = {
    item: PropTypes.shape({
        eid: PropTypes.number,
        uri: PropTypes.string,
        label: PropTypes.string,
        source: PropTypes.string,
        longitude: PropTypes.string,
        latitude: PropTypes.string,
        link: PropTypes.string,
    }).isRequired,
    onChange: PropTypes.func.isRequired,
    onDelete: PropTypes.func.isRequired,
};

function SameAsList({data, onItemChange}) {
    return (
        <ul className="list-group">
            {data.map((link, itemrank) => (link.toDelete !== true &&
                <li className="list-group-item" key={link.eid}>
                    <SameAsItem item={link}
                        onChange={(item) => onItemChange(item, itemrank)} />
                </li>
            ))}
        </ul>
    );
}

SameAsList.propTypes = {
    data: PropTypes.array.isRequired,
    onItemChange: PropTypes.func.isRequired,
};

class EditSameAsComp extends React.Component {
    constructor(props) {
        super(props);
        this.state = {data: null, loading: true, submitting: false,
                      shouldSubmit: false, errors:false};
        this.onItemChange = this.onItemChange.bind(this);
        this.onAdd = this.onAdd.bind(this);
        this.onSubmit = this.onSubmit.bind(this);
    }

    componentDidMount() {
        const {entity} = this.props,
            eid = entity.get('eid');
        Api.jsonFetch(`/fa/authority/${eid}/same_as`)
            .then(data => {
                if (data.length === 0) {
                    data = [{uri: '', label: ''}];
                }
                this.setState({data, loading: false});
            });
    }

    onSubmit(ev) {
        ev.preventDefault();
        const {data} = this.state,
            {entity} = this.props,
            eid = entity.get('eid');
        Api.jsonFetch(
            `/fa/authority/${eid}/same_as`,
            {method: 'PUT', body: JSON.stringify(data)}
        ).then(doc => {
            if (doc.errors && doc.errors.length) {
                this.setState({'errors': doc.errors});
            } else {
                document.location.reload();
            }
        })
        .catch(err => console.error('oups', err));
    }

    onAdd() {
        const {data} = this.state;
        data.push({uri: '', label: ''});
        this.setState({data, shouldSubmit: true});
    }

    onItemChange(item, itemrank) {
        const {data} = this.state,
            itemInData = data[itemrank];
        Object.assign(itemInData, item);
        this.setState({data, shouldSubmit: true});
    }

    onCancel(ev) {
        ev.preventDefault();
        document.location.reload();
    }

    render() {
        const {data, submitting, loading, shouldSubmit, errors} = this.state,
              {entity} = this.props,
              title = entity.get('dc_title'),
              i18netype = entity.get('i18n_cw_etype'),
              reltitle = 'pour ajouter des alignements';
        let errorsDiv = null;
        if (loading) {
            return (
                <div>
                    <h1>{title}</h1>
                    <h2>{reltitle}</h2>
                    <Spinner />
                </div>
            );
        }
        if (errors) {
            errorsDiv = <div className="panel panel-danger errors">
              <div className="panel-heading">
                  <h3 className="panel-title">Errors</h3>
                  <ul className="list-group">
                  { errors.map((error, i) => // eslint-disable-line no-unused-vars
                     <li className="list-group-item text-danger" key="{i}">
                     { error.details };
                    </li>
                  )}
                 </ul>
              </div>
            </div>
        }
        return (
            <div>
                <h1>
                    {i18netype} : &quot;{title}&quot;
                </h1>

                <div className="cms_add_link">
                    <button className="btn btn-default" onClick={this.onAdd}>Cliquer ici</button>
                    &nbsp;{reltitle}
                </div>
                { errors ? errorsDiv : null }
                <form className="form-horizontal" onSubmit={this.onSubmit}>
                    <SameAsList data={data} onItemChange={this.onItemChange} />
                    <button className="btn btn-default" onClick={this.onCancel}>annuler</button>
                    <button disabled={!shouldSubmit}className="btn btn-primary" type="submit">
                        {submitting ? <Spinner /> : null}
                        enregistrer
                    </button>
                </form>
            </div>
        );
    }
}


EditSameAsComp.propTypes = {
    entity: PropTypes.object.isRequired,
};


export const EditSameAs = connect(
    function mapStateToProps(state) {
        return {entity: state.getIn(['model', 'entity'])};
    },
)(EditSameAsComp);

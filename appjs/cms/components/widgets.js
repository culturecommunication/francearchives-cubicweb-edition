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
    PropTypes = require('prop-types');

const DateTimePicker = require('react-widgets/lib/DateTimePicker');

const Moment = require('moment'),
      _ = require('lodash'),
      MomentLocalizer = require('react-widgets/lib/localizers/moment');

const {default: {
    getAvailableTargets,
    getRelated,

}} = require('../api');

Moment.locale('fr');
MomentLocalizer(Moment);

const _localizers = require('react-widgets/lib/util/localizers');

const Select = require('react-select');

const {spinner} = require('../components/fa');

function isValid(d) {
    return !isNaN(d.getTime());
}

function formatDate(date, format, culture) {
    var val = null;
    if (date instanceof Date && isValid(date)) {
        val = _localizers.date.format(date, format, culture);
    }
    return val;
}

class DatePicker extends Component {
    constructor(props) {
        super(props);
        this.onChange = this.onChange.bind(this);
        var date = props.value === undefined ? null : new Date(props.value);
        this.state = {value: date};
    }

    onChange(date) {
        this.setState({value: date});
        return this.props.onChange(formatDate(date, 'YYYY-MM-DD'));
    }

    render() {
        return ce(DateTimePicker, {time: false,
                                   value: this.state.value,
                                   format: 'DD/MM/YYYY',
                                   onChange: this.onChange,
                                  }
                 );
    }
}
DatePicker.propTypes = {
    value: PropTypes.number,
    onChange: PropTypes.func.isRequired,
};


const Cropper = require('react-cropper').default;


function removePropsInDataUrl(value) {
    if (!value) {
        return {value: null, name: null};
    }
    const prefixIdx = value.indexOf(';'),
          prefix = value.slice(0, prefixIdx),
          base64Idx = value.indexOf(';base64'),
          suffix = value.slice(base64Idx);
    let name = null;
    for (let prop of value.slice(prefixIdx, base64Idx).split(';')) {
        if (prop.indexOf('=') === -1) {
            continue;
        }
        const [propName, propValue] = prop.split('=');
        if (propName === 'name') {
            name = propValue;
        }
    }
    return {value: prefix + suffix, name}
}

class ImagePicker extends Component {
    constructor(props) {
        super(props);
        const {value, name} = removePropsInDataUrl(props.value);
        this.state = {
            origSrc: value,
            src: value,
            croppedSrc: null,
            mimetype: null,
            name,
        };
        this.cropper = void 0;
        this.onUpload = this.onUpload.bind(this);
        this.cropImage = this.cropImage.bind(this);
        this.resetCrop = this.resetCrop.bind(this);
    }

    onUpload(e) {
        var _this = this;
        e.preventDefault();
        var files = void 0;
        if (e.dataTransfer) {
            files = e.dataTransfer.files;
        } else if (e.target) {
            files = e.target.files;
        }
        _this.setState({name: files[0].name});
        var reader = new FileReader();
        reader.onload = function() {
            var url = reader.result;
            _this.setState({src: url,
                            origSrc: url});
            _this.onChange(url, files[0].name);
        };
        reader.readAsDataURL(files[0]);
    }

    onChange(url, filename) {
        const base64Index = url.indexOf(';base64');
        url = url.slice(0, base64Index) + `;name=${filename}` +
            url.slice(base64Index);
        this.props.onChange(url);
    }

    cropImage() {
        if (typeof this.cropper.getCroppedCanvas() === 'undefined') {
            return null;
        }
        var url = this.cropper.getCroppedCanvas().toDataURL();
        this.setState({croppedSrc: url, src: url});
        return this.onChange(url, this.state.name);
    }

    resetCrop() {
        this.setState({src: this.state.origSrc,
                       croppedSrc: null});
        return this.onChange(this.state.src, this.state.name);
    }

    render() {
        return ce('div', null,
                  ce('input', {
                      type: 'file',
                      src: this.props.src,
                      onChange: this.onUpload,
                  }),
                  ce(Cropper, {
                      ref: (cropper) => {this.cropper = cropper;},
                      src: this.state.src,
                      style: {width: '100%'},
                      aspectRatio: 16 / 9,
                      preview: '.image-preview',
                  }),
                  ce('div', {style: {paddingTop: '1em',
                                     paddingBottom: '1em'}},
                     ce('a', {className: 'btn btn-default',
                              onClick: this.cropImage}, 'crop'),
                     ce('a', {className: 'btn btn-default',
                              onClick: this.resetCrop}, 'reset crop')
                    ),
                  ce('div', null,
                     ce('img', {
                         style: {width: '100%'},
                         src: this.state.croppedSrc})
                    )
                 );
    }
}
ImagePicker.propTypes = {
    value: PropTypes.string,
    src: PropTypes.string,
    onChange: PropTypes.func.isRequired,
};



class AutoCompleteField extends Component {
    constructor(props) {
        super(props);
        this.state = {
            rtype: props.name,
            formData: props.formData,
            required: props.required,
            related: [],
            multi: true, //FIXME
        };
        this.onChange = this.onChange.bind(this);
    }

    onChange(value) {
        const ids = value.map(e => String(e.value));
        this.setState({related: value,
                       formData: ids});
        this.props.onChange(ids);
    }

    componentDidMount() {
        const {rtype} = this.state,
              {cw_etype, eid} = this.props.formContext;
        if (eid !== undefined) {
            Promise.all([getRelated(cw_etype, eid, rtype)])
                .then(([related]) => {
                    // eslint-disable-next-line eqeqeq
                    if (this.state.formData == undefined) {
                        // lack of oneOf support
                        const ids = related.map(
                            r => (String(r.eid)));
                        this.setState({formData: ids});
                    }
                    const targets = related.map(
                        r => ({value: r.eid, label: r.dc_title}));
                    this.setState(
                        {related: targets});
                });
        } else {
            // eslint-disable-next-line eqeqeq
            if (this.state.formData == undefined) {
                this.setState({formData: []});
            }
        }

    }

    render() {
        const {rtype, required, related, multi} = this.state,
              {cw_etype, eid} = this.props.formContext;
        if (related === null) {
            return ce(spinner);
        }

        function loadOptions(input) {
                if (input.length < 3) {
                    return Promise.resolve({complete: false});
                }
            return getAvailableTargets(cw_etype, rtype, eid, input)
                .then(d => ({
                    complete: true,
                    options: d.map(e => ({label: e.title, value: e.eid})),
                }))
            }
        const labelClass = required ? 'control-label required': 'control-label';
        return ce('div', {className: 'form-group'},
                  ce('label', {className: labelClass}, rtype),
                  ce(Select.Async, {
                      multi: multi,
                      name: rtype,
                      loadOptions: _.throttle(loadOptions, 300),
                      value: related,
                      onChange: this.onChange,
                  }));
    }
}
AutoCompleteField.propTypes = {
    formContext: PropTypes.shape({
        cw_etype: PropTypes.string,
        eid: PropTypes.number,
    }),
    name: PropTypes.string,
    required: PropTypes.bool,
    formData: PropTypes.object,
    onChange: PropTypes.func.isRequired,
};



class FilePicker extends Component {
    constructor(props) {
        super(props);
        this.onChange = this.onChange.bind(this);
    }

    onChange(ev) {
        const file = ev.target.files[0];
        this.props.onChange(`${_.last(file.type.split('/'))}/${file.name}-${file.size}`);
    }

    render() {
        return ce('div', null,
                  'filepicker :',
                  ce('input', {type: 'file', onChange: this.onChange}));
    }
}

FilePicker.propTypes = {
    onChange: PropTypes.func.isRequired,
};


module.exports = {
    FilePicker, DatePicker, ImagePicker, AutoCompleteField,
};
